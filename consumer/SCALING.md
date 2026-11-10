# Consumer processing pool

The existing ten resort coordinator threads retain exclusive RabbitMQ consumers.
They assemble bounded batches and submit processing to one shared FIFO executor.
Each coordinator waits for its task to finish before submitting another, reconnecting,
or reusing its channels. Aggregation, confirmed outbox publication, and manual ACKs
run sequentially within that task. Existing schema and deduplication are unchanged.
HTTP threads, AMQP library threads, coordinator threads, and the scaler are separate
from the configured aggregation pool. Do not describe CONSUMER_THREADS as total JVM
threads or as additional consumers on each queue.

## Fixed experiments

Set CONSUMER_THREADS to 10, 20, 40, or 80 with
CONSUMER_DYNAMIC_SCALING=false (default). CONSUMER_MAX_THREADS defaults to
CONSUMER_THREADS. Threads are allocated on demand; a larger configured pool does
not imply that all threads are active. Ten resorts limit useful concurrent resort
aggregation tasks to ten. Extra capacity cannot parallelize a single resort.

## Dynamic experiment

CONSUMER_THREADS=2
CONSUMER_MIN_THREADS=2
CONSUMER_MAX_THREADS=10
CONSUMER_DYNAMIC_SCALING=true

The controller samples local active and queued batch tasks each second. Queued work
increases core pool capacity toward demand, bounded by the maximum. After five
consecutive samples with demand below capacity, capacity falls toward demand,
bounded by the minimum. This measures local batch scheduling pressure, not broker
queue depth. Connection backoff remains with the resort coordinators.

GET /stats retains uniqueSwipesProcessed and resorts and adds processingPool:
configuredThreads, maximumThreads, activeThreads, queuedBatches, dynamicScaling,
and resortParallelismLimit. Capture these values during experiments to distinguish
configured capacity from actual concurrency. Sampled stats are approximate.

## Validation and deployment

Run: mvn -f consumer/pom.xml clean package
18 tests cover existing store behavior and new scheduler behavior. Real RabbitMQ
recovery, channel behavior, and performance still require EC2 integration tests.
No speedup is claimed. WRITE_DELAY=0, batch settings, and manual ACK order remain.

Build and deploy using the existing JAR backup procedure. Start with fixed 10
threads and a small smoke test; verify committed count, all resort queues drained,
and final leaderboards before proceeding to 500K experiments. Set configuration in
a systemd EnvironmentFile, ensuring later overrides do not replace these settings.
Shutdown waits for processing to finish before closing stores; a systemd forced
kill may still interrupt a transaction, handled by durable deduplication on replay.
