package skitrak;
import com.google.gson.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class StandingsStoreOptimizationTest {
 @TempDir Path dir;
 String url(String name){return "jdbc:h2:file:"+dir.resolve(name)+";WRITE_DELAY=0;CACHE_SIZE=16384";}
 Swipe event(String day,String skier,int feet){Swipe e=new Swipe();e.eventId=UUID.randomUUID().toString();e.resortID="1";e.dayID=day;e.skierID=skier;e.liftID="1";e.vertical=feet;e.time=1;e.timestamp="2026-11-08T08:00:00Z";return e;}
 @Test void batchDeduplicatesWithinBatchAndAfterReopen() throws Exception {
  Swipe a=event("1","2",60),b=event("1","2",100);
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   assertEquals(List.of(true,false,true),s.applyBatch(List.of(a,a,b)));assertEquals(2,s.processed());assertEquals(160,s.total("1","2"));
  }
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   assertEquals(2,s.processed());assertNotEquals("{}",s.snapshot("1"));
   assertEquals(List.of(false,false),s.applyBatch(List.of(a,b)));assertEquals(160,s.total("1","2"));assertEquals(2,s.processed());
  }
 }
 @Test void collisionWithinBatchPublishesNoCountSnapshotOrTotal() throws Exception {
  Swipe a=event("1","1",60),b=event("1","1",100);b.eventId=a.eventId;
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   assertThrows(IllegalArgumentException.class,()->s.applyBatch(List.of(a,event("2","2",60),b)));
   assertEquals(0,s.processed());assertEquals("{}",s.snapshot("1"));assertEquals("{}",s.snapshot("2"));assertNull(s.nextUpdate());assertEquals(0,s.total("1","1"));
   assertTrue(s.apply(a));assertEquals(1,s.processed());
  }
 }
 @Test void persistedCollisionRollsBackOtherFreshEvents() throws Exception {
  Swipe a=event("1","1",60);
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   s.apply(a);String before=s.snapshot("1");a.vertical=100;
   assertThrows(IllegalArgumentException.class,()->s.applyBatch(List.of(event("1","2",200),a)));
   assertEquals(1,s.processed());assertEquals(before,s.snapshot("1"));assertEquals(0,s.total("1","2"));
  }
 }
 @Test void outboxFailureLeavesPreviouslyCommittedReadStateUntouched() throws Exception {
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   s.apply(event("1","1",60));String snapshot=s.snapshot("1");
   try(Connection c=DriverManager.getConnection(url("a"),"sa","");Statement q=c.createStatement()){q.execute("DROP TABLE outbox");}
   assertThrows(SQLException.class,()->s.applyBatch(List.of(event("1","1",100),event("2","2",60))));
   assertEquals(1,s.processed());assertEquals(60,s.total("1","1"));assertEquals(snapshot,s.snapshot("1"));assertEquals("{}",s.snapshot("2"));
  }
 }
 @Test void monitoringReadsDoNotWaitForStoreWriterMonitor() throws Exception {
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   s.apply(event("1","1",60));String expected=s.snapshot("1");
   ExecutorService pool=Executors.newSingleThreadExecutor();CountDownLatch held=new CountDownLatch(1),release=new CountDownLatch(1);
   try{
    Future<?> task=pool.submit(()->{synchronized(s){held.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}}});
    assertTrue(held.await(2,TimeUnit.SECONDS));
    org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(1),()->{assertEquals(1,s.processed());assertEquals(expected,s.snapshot("1"));});
    release.countDown();task.get(2,TimeUnit.SECONDS);
   }finally{release.countDown();pool.shutdownNow();}
  }
 }
 @Test void batchTotalsAndRankingMatchSequentialAcrossDays() throws Exception {
  Random random=new Random(42);List<Swipe> events=new ArrayList<>();
  for(int i=0;i<120;i++){Swipe e=event(Integer.toString(random.nextInt(3)+1),Integer.toString(random.nextInt(15)+1),random.nextInt(341)+60);events.add(e);if(i%9==0)events.add(e);}
  try(StandingsStore batched=new StandingsStore(url("batch"),1,10);StandingsStore sequential=new StandingsStore(url("single"),1,10)){
   batched.applyBatch(events);for(Swipe e:events)sequential.apply(e);
   assertEquals(sequential.processed(),batched.processed());
   for(int d=1;d<=3;d++){
    String day=Integer.toString(d);
    for(int k=1;k<=15;k++)assertEquals(sequential.total(day,Integer.toString(k)),batched.total(day,Integer.toString(k)));
    assertEquals(JsonParser.parseString(sequential.snapshot(day)).getAsJsonObject().get("top"),JsonParser.parseString(batched.snapshot(day)).getAsJsonObject().get("top"));
   }
  }
 }
 @Test void boundedLookupHandlesMoreThan1000EventsAndCanonicalIds() throws Exception {
  List<Swipe> events=new ArrayList<>();for(int i=0;i<1001;i++)events.add(event("1",Integer.toString(i%20+1),60));events.get(0).eventId=events.get(0).eventId.toUpperCase(Locale.ROOT);
  try(StandingsStore s=new StandingsStore(url("a"),1,10)){
   assertTrue(s.applyBatch(events).stream().allMatch(Boolean::booleanValue));assertEquals(1001,s.processed());
   assertTrue(s.applyBatch(events).stream().noneMatch(Boolean::booleanValue));assertEquals(1001,s.processed());
  }
 }
}
