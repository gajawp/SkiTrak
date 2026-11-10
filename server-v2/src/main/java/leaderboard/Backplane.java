package leaderboard;
import messaging.BrokerConfig;
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

/** One durable queue per WebSocket host; exclusive consumer prevents accidental sharing. */
public final class Backplane implements AutoCloseable {
    private static final Logger LOG=Logger.getLogger(Backplane.class.getName());
    private final LiveHub hub;
    private final String serverId;
    private final ExecutorService worker=Executors.newSingleThreadExecutor();
    private volatile Connection connection;
    private volatile boolean closed,ready;
    public Backplane(LiveHub hub,String serverId) {
        if(!serverId.matches("[A-Za-z0-9_-]{1,80}"))throw new IllegalArgumentException("SERVER_ID must use letters, digits, _ or -");
        this.hub=hub;this.serverId=serverId;
    }
    public void start(){worker.submit(this::run);}
    public boolean ready(){Connection c=connection;return ready&&c!=null&&c.isOpen();}
    private void run() {
        while(!closed) {
            try(Connection c=BrokerConfig.factory().newConnection("leaderboard-ws-"+serverId)) {
                connection=c;
                try(Channel ch=c.createChannel()) {
                    ch.exchangeDeclare("leaderboard.fanout",BuiltinExchangeType.FANOUT,true);
                    String queue="leaderboard.ws."+serverId;
                    ch.queueDeclare(queue,true,false,false,Map.of("x-max-length",100000,
                        "x-overflow","reject-publish","x-expires",86400000));
                    ch.queueBind(queue,"leaderboard.fanout","");ch.basicQos(50);
                    CountDownLatch ended=new CountDownLatch(1);
                    ch.addShutdownListener(cause->ended.countDown());
                    ch.basicConsume(queue,false,"ws-"+serverId,false,true,null,(tag,d)->{
                        try {
                            hub.accept(new String(d.getBody(),StandardCharsets.UTF_8));
                            ch.basicAck(d.getEnvelope().getDeliveryTag(),false);
                        } catch(RuntimeException invalid) {
                            LOG.warning("Rejecting invalid leaderboard snapshot");
                            ch.basicReject(d.getEnvelope().getDeliveryTag(),false);
                        }
                    },tag->ended.countDown());
                    ready=true;LOG.info("Leaderboard backplane ready: "+queue);
                    while(!closed&&c.isOpen()&&ch.isOpen())if(ended.await(1,TimeUnit.SECONDS))break;
                }
            } catch(Exception e){if(!closed)LOG.warning("Leaderboard backplane reconnect: "+e.getClass().getSimpleName());}
            finally{ready=false;connection=null;}
            if(!closed)try{Thread.sleep(2000);}catch(InterruptedException e){Thread.currentThread().interrupt();return;}
        }
    }
    @Override public void close(){closed=true;ready=false;Connection c=connection;if(c!=null)try{c.abort();}catch(Exception ignored){}worker.shutdownNow();}
}
