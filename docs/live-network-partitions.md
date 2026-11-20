# Live Kafka and RabbitMQ network partitions — Night PR #24

This CI experiment adds a **new failure mode** to Night's existing broker-kill tests:
the Docker network endpoint is disconnected while the Kafka or RabbitMQ broker
process **keeps running**. Each phase starts a new Java consumer/producer
process; PostgreSQL and broker on-disk data survive the fault.

The dedicated workflow is `.github/workflows/network-partition.yml`.
It uses three broker containers on one disposable GitHub Actions runner,
reusing the existing topologies in `tests/ha/*-three-node.yml` and the
existing real-broker integration tests.

## Two independently checked scenarios per broker

1. **Partition the actual topic/queue leader (2:1 topology).**
   Publish three events, commit and store the first event, then leave a second
   event unacknowledged. Identify the current broker leader, disconnect its
   Docker broker-network attachment while verifying its container is still
   running, and require the surviving two-node majority to elect a leader,
   confirm a new write, replay the unacknowledged events and retain the
   original PostgreSQL inbox event IDs. Reconnect the isolated broker **without
   restarting it**, verify the full Kafka in-sync replica set or RabbitMQ
   quorum availability, and confirm another message.
2. **Partition both followers while keeping the actual leader alive (1:2 topology).**
   Identify the *current* leader after healing, disconnect the other two
   broker-network endpoints while keeping all containers alive, and point a
   new Night producer at the still-running leader. **An isolated minority
   must not confirm a new durable write** (Kafka RF=3, min-ISR=2, acks=all;
   RabbitMQ 3-member quorum queue with publisher confirms). Preserve the five
   existing inbox records unchanged. Reconnect both followers without
   restarting any process, retry with the **same stable event ID** and check
   exactly one inbox row with its expected JSON payload.

The experiment fails if broker processes exit, a network endpoint is not
actually disconnected, the majority cannot make forward progress, a minority
reports confirmation, a committed message is lost, or the healed cluster
cannot safely accept the stable retry.

## Network fault mechanism

The bounded `tests/ha/network-partition.sh` script restricts itself to
the disposable `night-kafka-partition` and `night-rmq-partition` Docker
Compose projects. It discovers the running service containers via Compose,
uses `docker network disconnect --force` to sever their **broker** network,
and verifies through Docker inspection that the container remains running
and its network attachment was removed. Healing uses `docker network
connect --ip <original-IP> --alias <service>` and verifies the attachment is
restored. The script records the original broker IPv4 on the CI runner so
Erlang and Kafka peer connections do not inherit an artificial address
change when connectivity returns.

This is **real Docker bridge network disconnection**, rather than
`SIGKILL` or a hypothetical partition. It does not simulate arbitrary
latency, jitter, half-open TCP sockets, one-way packet loss, DNS poisoning,
or a multi-host firewall deployment. In particular, each disconnected
container temporarily has no broker-network endpoint; tests do not claim
an external client can still reach the isolated node through that lost
interface. The minority-negative test intentionally keeps the isolated
leader attached and accessible while both followers are disconnected.

Faults execute only inside dedicated disposable CI Docker projects and
never against production. The workflow collects cluster logs on failure,
attempts to reattach all broker networks in `always()` cleanup and deletes
the temporary containers and volumes afterwards. Repeated phases use
`NIGHT_HA_RUN_ID` to avoid accidentally reusing durable event namespaces.

## RabbitMQ network-partition handling

The RabbitMQ 4.1 default partition mode is `ignore`, which can leave
cluster metadata split after reconnecting a live isolated node. The lab
therefore applies the **explicit `pause_minority` policy**, recommended for
odd-sized RabbitMQ clusters, using
`tests/ha/rabbitmq-live-partition.override.yml` and
`rabbitmq-live-partition.conf`. CI verifies the effective setting on
all three nodes before starting the fault injection. The existing
RabbitMQ broker-kill workflow and Night's production adapter are not
reconfigured. See the RabbitMQ 4.1 documentation:
https://www.rabbitmq.com/docs/4.1/partitions

The workflow waits for the Erlang `net_tick_timeout` peer-loss
detection and actual quorum leader change before declaring surviving
majority availability; merely opening an AMQP socket is insufficient.
Before reconnecting an **isolated former leader**, CI also waits for
that node's own Erlang distribution logs to confirm `net_tick_timeout`
for **both** peers, using logs since the fault timestamp. This prevents
a race observed in CI where the majority was ready but the isolated
node had not yet recognized the partition: reconnecting at that
point produced `Mnesia inconsistent_database` events. After reconnecting,
the workflow waits for all three running cluster members before testing
new confirmed messages.

For the **one-node minority** negative test, the readiness check is
different: all three broker *containers* must still be running, the
current queue leader must retain its Docker network attachment, and
both other broker endpoints must be detached. The test does **not**
require the minority management API to respond or report only one
running node: `pause_minority` can deliberately stop the RabbitMQ
application and management listener without stopping the container.
The definitive safety assertion is that Night cannot obtain a
publisher confirmation, followed by an unchanged PostgreSQL inbox.
A stopped broker process or an unverified network cut is a test failure.

## Why PostgreSQL and message ID matter

A failed publisher acknowledgement is **ambiguous**: it is not proof
that the broker did not receive the message. Night uses Kafka offset
commit or RabbitMQ manual ACK only after durable PostgreSQL inbox
insertion. On healing, the same application event ID is safe to retry;
`(namespace, topic, event_id)` deduplication prevents a second
inbox row. Exactly-once *business-side effects* are still outside this
contract.

## Verification and remaining release gates

These tests complement:

- [Three-node quorum leader-kill and rejoin tests](three-node-ha.md)
- [Two-voter loss by broker process termination](quorum-majority-loss.md)
- [Broker restart and consumer handoff](broker-restart-tests.md)

Passing this workflow establishes repeatable **single-runner live network
partition and healing** regression evidence, but not cross-host
availability certification. Independent failure domains, TLS/SASL
reconfiguration and broker credential rotation, storage failures, rolling
upgrades, persistent lag/throughput observability, and production
load/latency SLOs remain outstanding.
