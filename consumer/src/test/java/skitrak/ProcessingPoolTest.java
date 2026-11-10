package skitrak;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
class ProcessingPoolTest {
    @Test void validatesBounds() {
        assertThrows(IllegalArgumentException.class,()->new ProcessingPool(0,1,10,false));
        assertThrows(IllegalArgumentException.class,()->new ProcessingPool(10,2,81,false));
    }
    @Test void propagatesFailureAndReusesPool() throws Exception {
        try(ProcessingPool p=new ProcessingPool(1,1,1,false)) {
            assertThrows(IllegalStateException.class,()->p.execute(()->{throw new IllegalStateException();}));
            AtomicInteger n=new AtomicInteger();p.execute(n::incrementAndGet);assertEquals(1,n.get());
        }
    }
    @Test void scalesQueuedWorkAndShrinksAfterQuietPeriod() throws Exception {
        try(ProcessingPool p=new ProcessingPool(1,1,4,false)) {
            ExecutorService callers=Executors.newFixedThreadPool(3);
            CountDownLatch entered=new CountDownLatch(3),release=new CountDownLatch(1);
            try {
                Future<?>[] tasks=new Future<?>[3];
                for(int i=0;i<3;i++)tasks[i]=callers.submit(()->{try{p.execute(()->{entered.countDown();release.await();});}catch(Exception e){throw new RuntimeException(e);}});
                long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while((int)p.stats().get("queuedBatches")<2 && System.nanoTime()<end)Thread.yield();
                assertEquals(2,p.stats().get("queuedBatches"));p.adjust();
                assertTrue(entered.await(3,TimeUnit.SECONDS));assertEquals(3,p.stats().get("configuredThreads"));
                release.countDown();for(Future<?> f:tasks)f.get(3,TimeUnit.SECONDS);
                for(int i=0;i<5;i++)p.adjust();assertEquals(1,p.stats().get("configuredThreads"));
            } finally {release.countDown();callers.shutdownNow();}
        }
    }
    @Test void interruptionDoesNotAbandonRunningBatch() throws Exception {
        try(ProcessingPool p=new ProcessingPool(1,1,1,false)) {
            CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
            AtomicInteger finished=new AtomicInteger();
            Thread caller=new Thread(()->{try{p.execute(()->{entered.countDown();release.await();finished.incrementAndGet();});assertTrue(Thread.currentThread().isInterrupted());}catch(Exception e){throw new RuntimeException(e);}});
            caller.start();assertTrue(entered.await(3,TimeUnit.SECONDS));caller.interrupt();
            assertEquals(0,finished.get());assertTrue(caller.isAlive());release.countDown();caller.join(3000);
            assertFalse(caller.isAlive());assertEquals(1,finished.get());
        }
    }
}
