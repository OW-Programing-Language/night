# Night messaging: quorum majority-loss fault injection

This incremental CI milestone extends the verified three-node leader
failover tests in [three-node-ha.md](three-node-ha.md). The experiment goes
further: rather than stopping **one** broker and checking majority
continuity, CI forcibly stops **two of three** broker/controller or quorum
queue members, leaving a **minority of one**.

## Safety objective

No application should report a newly confirmed broker write when the
cluster cannot satisfy its configured durable replication guarantees.
Night must surface the failure, not pretend a message was acknowledged.
When the majority is restored, the application can retry an uncertain
publish using the **same stable event ID**; PostgreSQL inbox
deduplication prevents replay from creating a second inbox row.

This check intentionally distinguishes **broker confirmation** from
**PostgreSQL acceptance**:

- No new PostgreSQL inbox entry is expected while the broker minority
  cannot confirm a new message.
- A producer timeout is **ambiguous**: broker ingestion may have started
  before timeout. Night does not claim that a failed confirmation proves
  no data was written; it means the client cannot claim a durable commit.
- On recovery, the test republishes the same event ID and verifies a
  single, unchanged inbox row.

## Kafka KRaft (three voters, RF=3, min-ISR=2)

The existing `.github/workflows/multi-node-ha.yml` Kafka job first
successfully runs its leader-loss test, restores three ISR replicas and
ingests events 1–5.

CI first identifies the **current actual topic partition leader** after the
previous failover; it then SIGKILLs the other two brokers/controllers,
leaving that leader reachable but isolated from a controller quorum and
short of the required two in-sync replicas. In a fresh Maven/JVM phase,
Night publishes event 6 with
`acks=all` and idempotent producer settings. The test demands a
**bounded failure**, because a lone broker/controller cannot establish
KRaft majority and cannot meet min-ISR=2. It also verifies the existing
five PostgreSQL inbox rows are unchanged.

After starting nodes 2 and 3 again, the next phase waits for three
in-sync replicas, republishes the same event ID, verifies confirmed
delivery, and checks exactly one inbox row for event 6 with the original
expected body.

## RabbitMQ quorum queue (3 members)

The existing RabbitMQ job also first completes its one-node leader-loss
test, restores the original three-member quorum and ingests events 1–5.

CI queries the RabbitMQ management API for the **current quorum queue
leader**, kills the two followers and keeps that leader's AMQP port reachable.
Night's optional three-member quorum queue is accessed on the minority
leader. Either queue access or publisher-confirmation
must fail; **a confirmed event 6 is a test failure**. No new event may
appear in the PostgreSQL inbox.

Bounded AMQP read timeouts and channel-shutdown errors prevent queue
operations from indefinitely hanging or falsely reporting confirmation.
After both followers restart and all authenticated RabbitMQ management
APIs report readiness, Night republishes the identical event ID,
receives a confirmed publication, and verifies one valid PostgreSQL
inbox row for event 6 and retention of all earlier events.

## Test boundaries

This is real **majority loss by broker process termination**, on a
single CI host. It is not a full asynchronous network-partition drill:
the removed nodes stop executing rather than remaining alive and
partitioned from one another. It does not establish split-brain
protection for every partition topology, production multi-host HA,
live TLS/SASL failover, disk corruption recovery, or a latency/throughput
SLO. Those remain open certification gates.

The job uses disposable broker clusters, loopback-only test credentials
and scoped event IDs; CI never runs these fault injections against
production. It fails on unexpected successful minority publication,
lost committed inbox rows, missing recovery, unbounded test execution,
or a broker failing to rejoin.

To reproduce the entire sequence, see the three phases in the
`kafka-quorum-ha` and `rabbitmq-quorum-ha` jobs of
`.github/workflows/multi-node-ha.yml`. Avoid rerunning a `before`
phase against the same durable namespace.

## Next lab milestone

Add **non-destructive network-isolation tests** that preserve
processes while severing links between a minority and majority;
include repeated heal/repartition cycles and persisted lag/latency
measurements. These require explicit Docker network-disconnect
or firewall controls and careful assertions about client reachability
versus cluster quorum.
