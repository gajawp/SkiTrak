# Real WebSocket leaderboard milestone

Requires the current consumer to be running. No consumer update is needed.
Set SERVER_ID uniquely per ingest host (ingest-1, ingest-2, etc.). Each host has its
own durable queue leaderboard.ws.<SERVER_ID>, bound to leaderboard.fanout.
Do NOT point multiple live hosts at the same SERVER_ID: their exclusive consumers
will conflict. Publishing and backplane receiving have separate AMQP connections;
the publisher retains its single-connection channel pool.

Build: mvn -f server-v2/pom.xml clean verify
WAR: server-v2/output/ski-servlet.war
Start Tomcat with existing RabbitMQ configuration plus:
CONSUMER_BASE_URL=http://localhost:9090
SERVER_ID=ingest-1

On EC2, CONSUMER_BASE_URL must use the consumer's reachable private address; its
HTTP service must bind a private interface via CONSUMER_HTTP_HOST. Firewall it
for ingest hosts. The default loopback configuration works for local testing.

Connect: ws://localhost:8080/ski-servlet/leaderboard/1
Day defaults to LEADERBOARD_DAY_ID (1); optionally append ?dayID=2.
The subscriber receives the current snapshot, if one exists, from cache or the
consumer API, then live updates from RabbitMQ. No synthetic rows are generated.
An empty resort/day does not send a fabricated snapshot: wait for a first swipe.
Health 200 requires both the publisher pool and backplane receiver to be ready.
It does not prove the separate consumer is healthy or that standings are fresh.

Snapshot versions increase per resort/day. Duplicate/old versions are ignored.
One asynchronous send per session is allowed at a time. For a slow client, pending
snapshots are coalesced to the newest; this is a latest-standings feed, not an event
log. Send failure closes that client so it can reconnect. The endpoint uses a
120-second idle timeout; heartbeat/keepalive tuning remains before long tests.

On subscription and every 5 seconds while subscribed, the server reconciles with
the consumer API. That repairs stale caches after reconnects or missed backplane
updates. If the API is down, cached/live RabbitMQ updates can still be delivered;
current initial state cannot be guaranteed until reconciliation succeeds.

serverTimestamp is generated immediately before the WebSocket send to preserve
the Assignment 1 client's push-latency meaning. aggregationTimestamp preserves
the consumer's original snapshot timestamp; sourceTimestamp preserves the event
ingest timestamp. Push latency is NOT end-to-end event processing latency.

Backplane queues reject new publications at 100,000 retained snapshots and expire
after 24 hours without use. Offline retained queues can backpressure all snapshot
publishers if they fill; remove queues for permanently retired hosts deliberately.
Queue expiration is repaired through the latest-snapshot API on return.

Local verification:
1. Connect wscat to the URL above; current test state should show version 2,
   skier 123 and 241 feet.
2. Post a NEW event ID ending 0003 for time 219 and liftID 3 (344 feet).
3. Observe version 3, vertical 585 in the open WebSocket.
4. Repeat the same event ID/body. No additional standings change should occur.
5. Open a second subscriber and confirm it receives the current snapshot.
6. Later, test subscribers on DIFFERENT ingest hosts: two subscribers on one
   host do not establish cross-host backplane correctness.

Validation: all 10 server tests passed, including four hub tests for resort/day
routing and stale versions, coalesced non-overlapping sends, cached initial state
and failed-send cleanup, and malformed snapshot rejection. WAR packaging verified.
Live Tomcat/WebSocket/RabbitMQ and multi-host recovery tests have not run here.
