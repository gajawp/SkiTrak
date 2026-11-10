package skitrak;
import com.rabbitmq.client.*;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/** One exclusive worker per resort; commits bounded batches in delivery order. */
public final class ResortWorker implements Runnable,AutoCloseable {
    public static final String BACKPLANE="leaderboard.fanout";
    private static final Logger LOG=Logger.getLogger(ResortWorker.class.getName());
    private final int resort;
    private final ProcessingPool processing;
    private final int batchSize;
    private final int batchWaitMillis;
    private final StandingsStore store;
    private final Gson gson=new Gson();
    private volatile boolean stopped,ready;
    private volatile Connection connection;
    public ResortWorker(int resort,StandingsStore store) {
        this(resort,store,null);
    }
    public ResortWorker(int resort,StandingsStore store,ProcessingPool processing) {
        this.processing=processing;
        this.resort=resort;
        this.store=store;
        batchSize=Config.number("CONSUMER_BATCH_SIZE",200);
        batchWaitMillis=Config.number("CONSUMER_BATCH_WAIT_MS",20);
        if(batchSize<1 || batchSize>1000 || batchWaitMillis<1 || batchWaitMillis>1000)
            throw new IllegalArgumentException("Batch size must be 1..1000; wait must be 1..1000 ms");
    }
    public boolean ready(){Connection c=connection;return ready && c!=null && c.isOpen();}
    @Override public void run() {
        while(!stopped) {
            try(Connection c=Config.factory().newConnection("skitrak-consumer-resort-"+resort)) {
                connection=c;
                try(Channel input=c.createChannel();Channel output=c.createChannel()) {
                    input.queueDeclarePassive("resort."+resort);
                    output.exchangeDeclare(BACKPLANE,BuiltinExchangeType.FANOUT,true);
                    // Retain confirmed updates until WebSocket wiring exists; also useful for evidence.
                    output.queueDeclare("leaderboard.audit",true,false,false,
                            Map.of("x-max-length",100000,"x-overflow","reject-publish"));
                    output.queueBind("leaderboard.audit",BACKPLANE,"");
                    output.confirmSelect();
                    AtomicBoolean returned=new AtomicBoolean();
                    output.addReturnListener((code,text,exchange,key,props,body)->returned.set(true));
                    BlockingQueue<Delivery> inbox=new ArrayBlockingQueue<>(batchSize);
                    // Exclusive consumption and a single processing thread preserve queue order.
                    input.basicQos(batchSize);
                    input.basicConsume("resort."+resort,false,"ordered-"+resort,false,true,null,
                            (tag,d)->inbox.add(d),tag-> {ready=false;try{c.abort();}catch(Exception ignored){}});
                    dispatch(()->flush(output,returned));
                    ready=true;
                    LOG.info("Resort "+resort+" consuming in order; batch="+batchSize+", waitMs="+batchWaitMillis);
                    while(!stopped && c.isOpen() && input.isOpen() && output.isOpen()) {
                        Delivery d=inbox.poll(200,TimeUnit.MILLISECONDS);
                        if(d==null){dispatch(()->flush(output,returned));continue;}
                        List<Delivery> batch=new ArrayList<>(batchSize);
                        batch.add(d);
                        long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(batchWaitMillis);
                        while(batch.size()<batchSize && !stopped) {
                            long remaining=deadline-System.nanoTime();
                            if(remaining<=0)break;
                            Delivery next=inbox.poll(remaining,TimeUnit.NANOSECONDS);
                            if(next==null)break;
                            batch.add(next);
                        }
                        dispatch(()->processBatch(batch,input,output,returned));
                    }
                }
            } catch(Exception ex) {
                if(!stopped)LOG.warning("Resort "+resort+" retrying in 2s: "+ex.getClass().getSimpleName());
            } finally {ready=false;connection=null;}
            if(!stopped)try{Thread.sleep(2000);}catch(InterruptedException e){Thread.currentThread().interrupt();return;}
        }
    }
    private void dispatch(ProcessingPool.Work work) throws Exception {
        if(processing==null)work.run();else processing.execute(work);
    }
    // No delivery is acknowledged before its durable transaction and outbox flush.
    void processBatch(List<Delivery> batch,Channel input,Channel output,
                      AtomicBoolean returned) throws Exception {
        List<Swipe> events=new ArrayList<>();
        Map<Delivery,Swipe> parsed=new LinkedHashMap<>();
        Set<Delivery> rejected=Collections.newSetFromMap(new IdentityHashMap<>());
        for(Delivery delivery:batch) {
            try {
                Swipe event=gson.fromJson(
                        new String(delivery.getBody(),StandardCharsets.UTF_8),Swipe.class);
                if(event==null)throw new IllegalArgumentException("Empty swipe");
                event.validate(resort);
                parsed.put(delivery,event);
                events.add(event);
            } catch(RuntimeException invalid) {
                rejected.add(delivery);
                LOG.warning("Dead-letter invalid event for resort "+resort);
            }
        }
        try {
            store.applyBatch(events);
        } catch(IllegalArgumentException collision) {
            // The atomic batch rolled back. Replay in the original order to isolate
            // collisions without dropping other deliveries or reordering their effects.
            for(Map.Entry<Delivery,Swipe> entry:parsed.entrySet()) {
                try {store.apply(entry.getValue());}
                catch(IllegalArgumentException invalid) {
                    rejected.add(entry.getKey());
                    LOG.warning("Dead-letter eventId collision for resort "+resort);
                }
            }
        }
        flush(output,returned);
        if(stopped || !input.isOpen() || !output.isOpen())
            throw new java.io.IOException("Worker stopped before acknowledgement");
        for(Delivery delivery:batch) {
            long tag=delivery.getEnvelope().getDeliveryTag();
            if(rejected.contains(delivery))input.basicReject(tag,false);
            else input.basicAck(tag,false);
        }
    }

    private void flush(Channel c,AtomicBoolean returned) throws Exception {
        Map.Entry<Long,String> update;
        while(!stopped && (update=store.nextUpdate())!=null) {
            returned.set(false);
            c.basicPublish(BACKPLANE,"",true,new AMQP.BasicProperties.Builder()
                    .contentType("application/json").deliveryMode(2)
                    .messageId(resort+":"+update.getKey()).build(),update.getValue().getBytes(StandardCharsets.UTF_8));
            c.waitForConfirmsOrDie(3000);
            if(returned.get())throw new java.io.IOException("Unroutable leaderboard");
            store.published(update.getKey());
        }
    }
    @Override public void close(){stopped=true;ready=false;Connection c=connection;if(c!=null)try{c.abort();}catch(Exception ignored){}}
}
