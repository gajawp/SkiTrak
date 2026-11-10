package skitrak;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Bounded, process-local metrics. No event IDs or timestamps are retained. */
public final class CommitMetrics {
    private final String since = Instant.now().toString();
    private long batches, freshEvents, negativeLagEvents, lagSamples, under100;
    private double durationSum, durationMax, lagSum, lagMin = Double.POSITIVE_INFINITY, lagMax;

    synchronized void record(Instant[] sources, int freshCount, Instant committedAt, long elapsedNanos) {
        batches++;
        freshEvents += freshCount;
        double durationMs = elapsedNanos / 1_000_000.0;
        durationSum += durationMs;
        durationMax = Math.max(durationMax, durationMs);
        for (int i = 0; i < freshCount; i++) {
            Duration age = Duration.between(sources[i], committedAt);
            double lagMs = age.getSeconds() * 1000.0 + age.getNano() / 1_000_000.0;
            if (lagMs < 0) { negativeLagEvents++; continue; }
            lagSamples++;
            lagSum += lagMs;
            lagMin = Math.min(lagMin, lagMs);
            lagMax = Math.max(lagMax, lagMs);
            if (lagMs < 100.0) under100++;
        }
    }

    public synchronized Map<String,Object> stats() {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("since", since);
        result.put("scope", "successful applyBatch calls since process start; historical DB events excluded");
        result.put("committedBatches", batches);
        result.put("freshEvents", freshEvents);
        Map<String,Object> processing = new LinkedHashMap<>();
        processing.put("definition", "applyBatch entry after store-lock acquisition through successful commit return; excludes queue and pool wait, ACK and outbox publication");
        processing.put("clock", "System.nanoTime");
        processing.put("meanMs", batches == 0 ? null : durationSum / batches);
        processing.put("maxMs", batches == 0 ? null : durationMax);
        result.put("batchProcessing", processing);
        Map<String,Object> lag = new LinkedHashMap<>();
        lag.put("definition", "ingest-created event timestamp to successful DB commit return, fresh events only; excludes ACK and WebSocket delivery");
        lag.put("requiresSynchronizedClocks", true);
        lag.put("samples", lagSamples);
        lag.put("negativeSamplesExcluded", negativeLagEvents);
        lag.put("meanMs", lagSamples == 0 ? null : lagSum / lagSamples);
        lag.put("minMs", lagSamples == 0 ? null : lagMin);
        lag.put("maxMs", lagSamples == 0 ? null : lagMax);
        lag.put("under100ms", under100);
        lag.put("under100msPercent", lagSamples == 0 ? null : 100.0 * under100 / lagSamples);
        result.put("eventToCommitLag", lag);
        return result;
    }
}
