package skitrak;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.UUID;
import com.google.gson.*;
import static org.junit.jupiter.api.Assertions.*;
class StandingsStoreTest {
    @TempDir Path dir;
    Swipe event(String skier,int feet) {
        Swipe e=new Swipe();e.eventId=UUID.randomUUID().toString();e.resortID="1";e.dayID="1";
        e.skierID=skier;e.liftID="1";e.vertical=feet;e.time=217;e.timestamp="2026-11-01T23:06:19Z";e.serverId="ingest-1";return e;
    }
    String url(){return "jdbc:h2:file:"+dir.resolve("resort-1")+";WRITE_DELAY=0";}
    @Test void twoSwipesTotal241AndRedeliveryDoesNotAddAgain() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            Swipe a=event("123",167),b=event("123",74);
            assertTrue(s.apply(a));assertTrue(s.apply(b));
            a.serverId="ingest-2";a.timestamp="2026-11-01T23:09:19Z";
            assertFalse(s.apply(a));assertEquals(241,s.total("1","123"));assertEquals(2,s.processed());
        }
    }
    @Test void restartPreservesTotalsDedupeAndPendingPublication() throws Exception {
        Swipe e=event("123",167);long seq;
        try(StandingsStore s=new StandingsStore(url(),1,10)) {s.apply(e);seq=s.nextUpdate().getKey();}
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            assertFalse(s.apply(e));assertEquals(167,s.total("1","123"));
            assertEquals(seq,s.nextUpdate().getKey());s.published(seq);assertNull(s.nextUpdate());
            assertFalse(s.apply(e));assertNull(s.nextUpdate());
        }
    }
    @Test void topNChangesOnlyAndNumericTieBreak() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,2)) {
            s.apply(event("10",200));s.apply(event("2",200));
            while(s.nextUpdate()!=null)s.published(s.nextUpdate().getKey());
            s.apply(event("30",60));assertNull(s.nextUpdate());
            JsonArray top=JsonParser.parseString(s.snapshot("1")).getAsJsonObject().getAsJsonArray("top");
            assertEquals("2",top.get(0).getAsJsonObject().get("skierID").getAsString());
            s.apply(event("30",200));assertNotNull(s.nextUpdate());
            top=JsonParser.parseString(s.snapshot("1")).getAsJsonObject().getAsJsonArray("top");
            assertEquals("30",top.get(0).getAsJsonObject().get("skierID").getAsString());
        }
    }
    @Test void daysRemainSeparateAndIdCollisionRollsBack() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            Swipe a=event("123",167);s.apply(a);a.vertical=74;
            assertThrows(IllegalArgumentException.class,()->s.apply(a));assertEquals(167,s.total("1","123"));
            Swipe b=event("123",74);b.dayID="2";s.apply(b);
            assertEquals(74,s.total("2","123"));assertEquals(2,s.processed());
        }
    }
    @Test void rejectsWrongResortBeforeCommitting() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            Swipe e=event("123",167);e.resortID="2";
            assertThrows(IllegalArgumentException.class,()->s.apply(e));assertEquals(0,s.processed());
        }
    }
    @Test void outboxWriteFailureRollsBackDedupeAndTotals() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            try(java.sql.Connection c=java.sql.DriverManager.getConnection(url(),"sa","");
                java.sql.Statement q=c.createStatement()) {q.execute("DROP TABLE outbox");}
            assertThrows(java.sql.SQLException.class,()->s.apply(event("123",167)));
            assertEquals(0,s.processed());assertEquals(0,s.total("1","123"));
            assertEquals("{}",s.snapshot("1"));
        }
    }
    @Test void batchCutsOneSnapshotPerDayWithFinalStandings() throws Exception {
        try(StandingsStore s=new StandingsStore(url(),1,10)) {
            s.applyBatch(java.util.List.of(event("1",60),event("2",100),event("1",200)));
            long seq=s.nextUpdate().getKey();s.published(seq);assertNull(s.nextUpdate());
            JsonObject snap=JsonParser.parseString(s.snapshot("1")).getAsJsonObject();
            assertEquals(1,snap.get("version").getAsLong());
            assertEquals("1",snap.getAsJsonArray("top").get(0).getAsJsonObject().get("skierID").getAsString());
            assertEquals(260,s.total("1","1"));assertEquals(3,s.processed());
        }
    }
}
