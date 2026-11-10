package leaderboard;
import com.google.gson.*;
import javax.websocket.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/** Local subscribers, monotonic snapshots and one coalesced pending update per client. */
public final class LiveHub implements AutoCloseable {
    public static volatile LiveHub current;
    private static final Logger LOG=Logger.getLogger(LiveHub.class.getName());
    private final Map<String,Snapshot> latest=new ConcurrentHashMap<>();
    private final Map<Session,Subscriber> subscribers=new ConcurrentHashMap<>();
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final ScheduledExecutorService refresh=Executors.newScheduledThreadPool(2);
    private final String consumerUrl;
    private volatile boolean closed;
    public LiveHub(String url){consumerUrl=url.replaceAll("/+$","");}
    public void start(){refresh.scheduleWithFixedDelay(this::refreshAll,1,5,TimeUnit.SECONDS);}
    public void add(Session session,String resort,String day) {
        if(closed)throw new IllegalStateException("Server stopping");
        session.getAsyncRemote().setSendTimeout(2000);
        session.setMaxIdleTimeout(120000);
        Subscriber sub=new Subscriber(session,resort+":"+day);
        subscribers.put(session,sub);
        Snapshot cached=latest.get(sub.key);if(cached!=null)sub.offer(cached);
        try{refresh.execute(()->refreshOne(resort,day));}catch(RejectedExecutionException ignored){}
    }
    public void remove(Session session){subscribers.remove(session);}
    public void accept(String json) {
        Snapshot incoming=Snapshot.parse(json);
        // compute serializes cache changes for this key. Per-subscriber version checks
        // also fence concurrent HTTP refresh and RabbitMQ callbacks.
        latest.compute(incoming.key(),(key,previous)->
            previous==null||incoming.version>previous.version?incoming:previous);
        Snapshot selected=latest.get(incoming.key());
        for(Subscriber sub:subscribers.values())if(sub.key.equals(incoming.key()))sub.offer(selected);
    }
    private void refreshAll() {
        if(closed)return;
        Set<String> keys=new HashSet<>();for(Subscriber s:subscribers.values())keys.add(s.key);
        for(String key:keys){String[] p=key.split(":");refreshOne(p[0],p[1]);}
    }
    private void refreshOne(String resort,String day) {
        if(closed)return;
        try {
            HttpRequest req=HttpRequest.newBuilder(URI.create(consumerUrl+"/leaderboard/"+resort+"/"+day))
                .timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> res=http.send(req,HttpResponse.BodyHandlers.ofString());
            if(res.statusCode()==200 && !res.body().trim().equals("{}"))accept(res.body());
        } catch(InterruptedException e){Thread.currentThread().interrupt();}
        catch(Exception e){LOG.fine("Leaderboard refresh unavailable: "+e.getClass().getSimpleName());}
    }
    private final class Subscriber {
        final Session session;final String key;
        Snapshot pending;boolean sending;long newest=0;
        Subscriber(Session s,String k){session=s;key=k;}
        synchronized void offer(Snapshot snapshot) {
            if(closed||snapshot.version<=newest||!session.isOpen())return;
            newest=snapshot.version;pending=snapshot;
            if(!sending)sendNext();
        }
        private synchronized void sendNext() {
            if(closed||!session.isOpen()){remove(session);return;}
            if(pending==null){sending=false;return;}
            Snapshot snapshot=pending;pending=null;sending=true;
            JsonObject payload=JsonParser.parseString(snapshot.json).getAsJsonObject();
            payload.add("aggregationTimestamp",payload.get("serverTimestamp"));
            payload.addProperty("serverTimestamp",Instant.now().toString());
            try {
                session.getAsyncRemote().sendText(payload.toString(),result->{
                    synchronized(Subscriber.this) {
                        if(!result.isOK()){remove(session);closeSession(session);sending=false;pending=null;}
                        else sendNext();
                    }
                });
            } catch(RuntimeException e){remove(session);closeSession(session);sending=false;pending=null;}
        }
    }
    private static void closeSession(Session s){try{s.close();}catch(IOException ignored){}}
    @Override public void close(){closed=true;refresh.shutdownNow();for(Session s:subscribers.keySet())closeSession(s);subscribers.clear();latest.clear();}
}
