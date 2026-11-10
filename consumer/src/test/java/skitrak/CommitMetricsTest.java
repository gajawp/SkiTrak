package skitrak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class CommitMetricsTest {
    @TempDir Path dir;
    @SuppressWarnings("unchecked")
    private Map<String,Object> lag(CommitMetrics m) {
        return (Map<String,Object>)m.stats().get("eventToCommitLag");
    }
    private Swipe event() {
        Swipe e=new Swipe(); e.eventId=UUID.randomUUID().toString(); e.resortID="1";
        e.dayID="1"; e.skierID="123"; e.liftID="1"; e.vertical=100; e.time=1;
        e.timestamp="2026-01-01T00:00:00Z"; e.serverId="ingest-1"; return e;
    }
    @Test void signedClockSkewIsExcludedAndStrictThresholdIsExplicit() {
        CommitMetrics m=new CommitMetrics(); Instant end=Instant.parse("2026-01-01T00:00:01Z");
        m.record(new Instant[]{end.minusMillis(99),end.minusMillis(100),end.plusMillis(1)},3,end,2_000_000);
        assertEquals(3L,m.stats().get("freshEvents"));
        assertEquals(2L,lag(m).get("samples")); assertEquals(1L,lag(m).get("negativeSamplesExcluded"));
        assertEquals(1L,lag(m).get("under100ms")); assertEquals(50.0,lag(m).get("under100msPercent"));
        assertEquals(99.5,lag(m).get("meanMs"));
    }
    @Test void duplicatesDoNotCreateLagSamplesAndCollisionDoesNotRecord() throws Exception {
        CommitMetrics m=new CommitMetrics();
        try(StandingsStore s=new StandingsStore("jdbc:h2:file:"+dir.resolve("dedup"),1,10,m)) {
            Swipe e=event(); s.applyBatch(List.of(e,e));
            assertEquals(1L,m.stats().get("freshEvents"));
            s.apply(e); assertEquals(1L,lag(m).get("samples"));
            long batches=(Long)m.stats().get("committedBatches"); e.vertical=200;
            assertThrows(IllegalArgumentException.class,()->s.apply(e));
            assertEquals(batches,m.stats().get("committedBatches"));
        }
    }
    @Test void sqlRollbackDoesNotRecordLagOrBatch() throws Exception {
        String url="jdbc:h2:file:"+dir.resolve("rollback"); CommitMetrics m=new CommitMetrics();
        try(StandingsStore s=new StandingsStore(url,1,10,m)) {
            try(java.sql.Connection c=java.sql.DriverManager.getConnection(url,"sa","");
                java.sql.Statement q=c.createStatement()) {q.execute("DROP TABLE outbox");}
            assertThrows(java.sql.SQLException.class,()->s.apply(event()));
            assertEquals(0L,m.stats().get("committedBatches")); assertEquals(0L,lag(m).get("samples"));
            assertNull(lag(m).get("meanMs"));
        }
    }
    @Test void metricsResetWithoutCountingHistoricalEventsOnRestart() throws Exception {
        String url="jdbc:h2:file:"+dir.resolve("restart"); Swipe e=event();
        try(StandingsStore s=new StandingsStore(url,1,10,new CommitMetrics())) {s.apply(e);}
        CommitMetrics m=new CommitMetrics();
        try(StandingsStore s=new StandingsStore(url,1,10,m)) {
            assertEquals(1,s.processed()); assertEquals(0L,m.stats().get("freshEvents"));
            s.apply(e); assertEquals(0L,lag(m).get("samples"));
        }
    }
}
