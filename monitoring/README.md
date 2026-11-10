# Metrics for subsequent load tests
Capture ready + unacknowledged messages per resort, publish/deliver/ack rates,
oldest event age and timestamp-to-commit lag, unique committed event IDs,
dead-letter counts, resource utilization and per-ingest accepted counts.
A 201 confirms publication, not aggregation. Drain queues and reconcile unique events
and expected vertical totals before claiming no swipe loss. Inspect the dead-letter queue:
TTL and queue limits must never silently discard accepted swipes in test evidence.
