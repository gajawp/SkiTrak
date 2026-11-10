# Consumer milestone: ordered aggregation and durable deduplication

Run from the project root:

```bash
mvn -f consumer/pom.xml clean verify
export RABBITMQ_HOST=localhost
export RABBITMQ_USER=skitrak
export RABBITMQ_PASSWORD='SkiTrakLocal2026!'
java -jar consumer/target/consumer-1.0-SNAPSHOT.jar
```

Start the ingest server first so the ten resort queues exist. Keep the consumer
terminal running; use a second terminal for curl. Only run one consumer process
against these queues. This implementation uses one exclusive classic-queue
consumer per resort, 10 worker threads, prefetch 1, and separate connections per
resort. Channel pooling requirements remain implemented in the ingest server.

```bash
curl -i http://localhost:9090/health
curl http://localhost:9090/stats
curl http://localhost:9090/leaderboard/1/1
```

With only the two earlier test events present, skier 123 totals 241 feet and
uniqueSwipesProcessed is 2. If an event was removed or expired, replay it using
its original X-Event-ID and original body. Replaying a processed event does not
increase the count or totals. Do not reuse that ID for a different swipe.

Inspect `leaderboard.audit` in RabbitMQ to see real generated snapshots. Select
Nack/requeue when inspecting. The consumer publishes to durable fanout exchange
`leaderboard.fanout`. The audit queue retains copies for this milestone; WebSocket
servers will each bind their OWN queue in the next milestone. Publishing to the
backplane does not yet deliver anything to a WebSocket client.

Persistence: files live in `consumer-data` under the working directory. Always
restart from the same project root, or set CONSUMER_DATA_DIR to one fixed absolute
path. Keep that directory, do not commit it, and do not run two processes sharing
it. Stop using Ctrl+C. Restarting retains totals, dedupe IDs, versions, and pending
updates. This is local-disk persistence; it is not replicated or host failover.

Processing order: validate, transactionally save dedupe + totals + changed top-N
snapshot + outbox, confirm publishing pending updates, delete confirmed outbox
rows, manually acknowledge input. A crash between confirm and outbox deletion can
republish a snapshot: future WebSocket servers must ignore older/equal versions
per (resort, day). Updates contain resortID, dayID, version, serverTimestamp,
sourceTimestamp and top entries. Same-vertical ties sort by numeric skier ID.

Malformed events and IDs reused with conflicting payloads are rejected without
requeue and routed to the existing `skitrak.dead` dead-letter exchange/queue.
Transient broker/storage failures close the connection, leave input unacked,
and retry after two seconds. Retrying the same unacked event cannot count it twice.
An operational storage failure retries indefinitely; investigate logs rather than
deleting state. No later message for a resort is processed while that delivery
is unacknowledged. Do not use management Get Messages on input queues during
ordering tests; those requests can change delivery order.

Configuration: TOP_N (default 10), CONSUMER_DATA_DIR (default consumer-data),
CONSUMER_HTTP_HOST (127.0.0.1), CONSUMER_HTTP_PORT (9090), and the same RabbitMQ
connection variables as the ingest server. Bind health to a private interface
when deploying remotely. Never expose this unauthenticated inspection API publicly.

Current boundaries: fixed 10 ordered workers and prefetch 1 are a correctness
baseline. Configurable/dynamic worker assignment, 10/20/40/80 tuning, batch acks,
load profiling, EC2 health/auto-restart, and WebSocket wiring remain to implement.
The audit queue rejects publication once 100,000 retained updates accumulate;
drain it with an acknowledging consumer before long load tests. Do not treat
it as an indefinite production archive. Keep TOP_N unchanged across restarts
of an existing data directory for this milestone.

Validation: six automated tests passed (241 total/dedupe, restart and pending
outbox recovery, top-N threshold/ties, day isolation/collision rollback, wrong
resort rejection, and rollback on an outbox write failure). Live RabbitMQ processing and load/reconnect tests must run
on your Mac; mocks/unit tests do not establish end-to-end guarantees.

References:
https://www.rabbitmq.com/docs/4.1/consumers
https://www.rabbitmq.com/docs/confirms
https://h2database.com/html/commands.html
