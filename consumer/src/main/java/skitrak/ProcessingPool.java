package skitrak;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared FIFO batch executor. A resort submits only one task at a time. */
public final class ProcessingPool implements AutoCloseable {
    @FunctionalInterface public interface Work { void run() throws Exception; }
    private final ThreadPoolExecutor executor;
    private final ScheduledExecutorService scaler;
    private final int minimum, maximum;
    private final boolean dynamic;
    private int quietTicks;

    public ProcessingPool(int initial, int minimum, int maximum, boolean dynamic) {
        if (minimum < 1 || maximum > 80 || minimum > initial || initial > maximum)
            throw new IllegalArgumentException("Require 1 <= min <= threads <= max <= 80");
        this.minimum=minimum; this.maximum=maximum; this.dynamic=dynamic;
        AtomicInteger ids=new AtomicInteger();
        // At most ten resort coordinators can submit work; FIFO avoids starvation.
        executor=new ThreadPoolExecutor(initial,maximum,30,TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),r->new Thread(r,"aggregation-"+ids.incrementAndGet()));
        scaler=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"consumer-scaler"));
        if(dynamic)scaler.scheduleWithFixedDelay(this::adjust,1,1,TimeUnit.SECONDS);
    }

    public void execute(Work work) throws Exception {
        Future<?> task=executor.submit(()->{work.run();return null;});
        boolean interrupted=false;
        try {
            // Do not abandon a still-running batch on interruption: its channel and
            // store must not be reused or closed until the task actually finishes.
            for(;;)try {task.get();return;}
            catch(InterruptedException e){interrupted=true;}
            catch(ExecutionException e){
                Throwable cause=e.getCause();
                if(cause instanceof Exception)throw (Exception)cause;
                if(cause instanceof Error)throw (Error)cause;
                throw new RuntimeException(cause);
            }
        } finally {if(interrupted)Thread.currentThread().interrupt();}
    }

    synchronized void adjust() {
        int current=executor.getCorePoolSize();
        int demand=executor.getActiveCount()+executor.getQueue().size();
        if(!executor.getQueue().isEmpty() && current<maximum) {
            executor.setCorePoolSize(Math.min(maximum,Math.max(current+1,demand)));
            quietTicks=0;
        } else if(demand<current) {
            if(++quietTicks>=5) {
                executor.setCorePoolSize(Math.max(minimum,Math.max(1,demand)));
                quietTicks=0;
            }
        } else quietTicks=0;
    }

    public Map<String,Object> stats() {
        return Map.of("configuredThreads",executor.getCorePoolSize(),
                "activeThreads",executor.getActiveCount(),"queuedBatches",executor.getQueue().size(),
                "maximumThreads",maximum,"dynamicScaling",dynamic,"resortParallelismLimit",10);
    }
    @Override public void close() {
        scaler.shutdownNow();executor.shutdown();
        boolean interrupted=false;
        for(;;)try {if(executor.awaitTermination(1,TimeUnit.SECONDS))break;}
        catch(InterruptedException e){interrupted=true;}
        if(interrupted)Thread.currentThread().interrupt();
    }
}
