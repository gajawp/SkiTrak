import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public final class SwipeGenerator implements Runnable {

    private final BlockingQueue<SwipeEvent> queue;
    private final int eventCount;
    private final int resortCount;
    private final int seasonID;
    private final int dayID;

    private volatile int generatedCount;
    private volatile RuntimeException failure;

    public SwipeGenerator(BlockingQueue<SwipeEvent> queue,
                          int eventCount,
                          int resortCount,
                          int seasonID,
                          int dayID) {

        if (queue == null) {
            throw new IllegalArgumentException("queue cannot be null");
        }
        if (eventCount <= 0 || resortCount <= 0
                || seasonID <= 0 || dayID <= 0) {
            throw new IllegalArgumentException(
                    "All configuration values must be positive");
        }

        this.queue = queue;
        this.eventCount = eventCount;
        this.resortCount = resortCount;
        this.seasonID = seasonID;
        this.dayID = dayID;
    }

    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        try {
            for (int i = 0; i < eventCount; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            "Swipe generation was interrupted");
                }

                SwipeEvent event = new SwipeEvent(
                        random.nextInt(100_000) + 1,
                        random.nextInt(resortCount) + 1,
                        random.nextInt(40) + 1,
                        seasonID,
                        dayID,
                        random.nextInt(360) + 1
                );

                // The queue must have room for all events because
                // generation completes before senders start.
                if (!queue.offer(event)) {
                    throw new IllegalStateException(
                            "Insufficient queue capacity for swipe generation");
                }

                generatedCount++;
            }
        } catch (RuntimeException error) {
            failure = error;
        }
    }

    public int getGeneratedCount() {
        return generatedCount;
    }

    public void checkCompleted() {
        if (failure != null) {
            throw new IllegalStateException(
                    "Swipe generation failed", failure);
        }

        if (generatedCount != eventCount) {
            throw new IllegalStateException(
                    "Expected " + eventCount
                            + " events, generated " + generatedCount);
        }
    }
}