# Three-node broker leader failover: disposable CI lab

Night verifies **actual broker quorum membership, leader loss and durable
message continuation** in `.github/workflows/multi-node-ha.yml`. This is a
stronger scenario than [single-broker restart tests](broker-restart-tests.md):
there are three Kafka broker/controllers or three separate RabbitMQ nodes,
and the test **kills the current leader** while the two other nodes remain
running. PostgreSQL is shared across the test phases; each phase runs in a
fresh JVM.

**Scope:** three containers on a single GitHub runner are not independent
failure domains. Network partitions, different physical hosts, storage
failure, TLS certificate rotation, multiple simultaneous failures and
production throughput SLOs are not certified by these tests.

## Kafka / KRaft 3.9.1

`tests/ha/kafka-three-node.yml` is a three-voter, combined
broker/controller cluster based on the official Apache KRaft topology.
Topics are explicitly provisioned with:

- Replication factor **3** and **one partition** for deterministic failure injection
- `min.insync.replicas=2`, plus producer `acks=all` and idempotence
- `__consumer_offsets` replication factor **3**
- Real group offset commits, no automatic Kafka consumer offset ACK

`NightKafkaQuorumFailoverTest` asserts **three ISR members** before
publishing three events. A separate Java consumer writes event 1 into the
PostgreSQL inbox, commits offset 1 and then deliberately leaves event 2
uncommitted. The test records the **actual topic partition leader node ID**.
CI kills that leader's broker/controller container using SIGKILL.

After the remaining two nodes elect a leader, a new JVM checks **two ISR
members**, publishes a fourth event with `acks=all`, recovers unacknowledged
events 2 and 3, and confirms that the previously committed event 1 did not
replay. A further restart checks that the group offset is still committed.
The stopped broker is then returned, the test waits for **three ISR members**
and confirms a fifth event.

A failure in broker election, lost events, a stale group offset, insufficient
replication, or changed inbox event contents fails the job.

## RabbitMQ 4.1 quorum queues

Night's `NightRabbitMQ` adapter now supports an **opt-in queue type**:

```sh
export NIGHT_RABBITMQ_QUEUE_TYPE=quorum
```

The default remains `classic` for existing deployments. Queue types
cannot be switched in place: migrate traffic to a newly declared queue and
change worker configuration only as part of a planned cutover.

`tests/ha/rabbitmq-three-node.yml` runs three nodes sharing a CI-only
Erlang cookie. CI explicitly joins nodes 2 and 3 to node 1, checks actual
cluster membership, and declares a three-member durable quorum queue.
Each publication uses publisher confirmations. The source application
connects to node 1 to store and ACK event 1, and leaves event 2 unacknowledged
when its connection closes.

CI inspects `rabbitmq-queues quorum_status` and the management API to
verify three queue members and identify the **actual leader**. It kills the
leader's container rather than just a chosen arbitrary node. A new Java
process connects to a **surviving broker node** and must be able to publish
event 4 with confirmation, recover messages 2 and 3, and preserve exactly
one PostgreSQL inbox record per event ID. Repeated event IDs must be marked
`DUPLICATE` and may not overwrite the persisted body. The crashed node
then returns, followed by an additional publication and inbox check.

Quorum queue membership and a functioning surviving majority are required;
restarting a lone classic queue is **not** treated as a successful quorum
test. Broker clients are re-created after the leader is killed: the exercise
does not guarantee uninterrupted connection migration in an existing OW
process.

## Running and interpreting the test

The workflow uses separate, isolated Kafka and RabbitMQ jobs. Each starts a
disposable PostgreSQL 16 service and installs the pinned OW runtime SPI.
The integration tests use explicit `night.ha.phase` before/after/return
values and are never intended to run against production infrastructure.
Unique GitHub run IDs create isolated database namespaces, topics and queues.
All broker containers are torn down in `always()` steps.

For local investigation use `docker compose -f tests/ha/kafka-three-node.yml`
or `docker compose -f tests/ha/rabbitmq-three-node.yml`, then follow
`.github/workflows/multi-node-ha.yml` precisely for the staged commands,
leader identification, fault injection and cleanup. In particular, do not
rerun the `before` phase against an existing test namespace or point the
fault-injection script at a production broker.

The cluster lab uses **plaintext on localhost and a fixed CI-only
password/cookie**. Real clusters require TLS/mTLS or SASL, ACLs, credential
rotation, secure discovery, replication monitoring and truly independent
failure domains.

## Still outside the release certification gate

- Real network partitions or majority loss, and detecting that a minority
  must reject writes
- Recovery from physical disk failure or loss of broker log volumes
- Kubernetes rolling upgrades, rebalance and rescheduling between hosts
- Production TLS/SASL broker failover and certificate rotation
- Per-broker metrics, realistic sustained throughput, latency SLOs and soak
- End-to-end business transactions and independent security audit

See [Night 1.0 release gates](release-gates.md) and
[the tracked roadmap](https://github.com/OW-Programing-Language/night/issues/3).
