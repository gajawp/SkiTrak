package skitrak;
import com.sun.net.httpserver.HttpServer;
import com.google.gson.Gson;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
public final class ConsumerMain {
    public static void main(String[] args) throws Exception {
        Path data=Paths.get(Config.env("CONSUMER_DATA_DIR","consumer-data")).toAbsolutePath();
        Files.createDirectories(data);
        CommitMetrics metrics=new CommitMetrics();
        int topN=Config.number("TOP_N",10);
        int cacheKB=Config.number("CONSUMER_DB_CACHE_KB",16384);
        int initial=Config.number("CONSUMER_THREADS",10);
        ProcessingPool processing=new ProcessingPool(initial,
                Config.number("CONSUMER_MIN_THREADS",Math.min(initial,2)),
                Config.number("CONSUMER_MAX_THREADS",initial),
                Boolean.parseBoolean(Config.env("CONSUMER_DYNAMIC_SCALING","false")));
        List<StandingsStore> stores=new ArrayList<>();List<ResortWorker> workers=new ArrayList<>();
        List<Thread> threads=new ArrayList<>();
        for(int resort=1;resort<=10;resort++) {
            StandingsStore s=new StandingsStore("jdbc:h2:file:"+data.resolve("resort-"+resort)+";WRITE_DELAY=0;CACHE_SIZE="+cacheKB,resort,topN,metrics);
            stores.add(s);ResortWorker worker=new ResortWorker(resort,s,processing);workers.add(worker);
            Thread t=new Thread(worker,"resort-"+resort);threads.add(t);t.start();
        }
        HttpServer http=HttpServer.create(new InetSocketAddress(Config.env("CONSUMER_HTTP_HOST","127.0.0.1"),
                Config.number("CONSUMER_HTTP_PORT",9090)),0);
        ExecutorService httpThreads=Executors.newFixedThreadPool(2);http.setExecutor(httpThreads);
        http.createContext("/",ex->{
            int status=200;String body;
            try {
                String path=ex.getRequestURI().getPath();
                if(!"GET".equals(ex.getRequestMethod())) {status=405;body="{}";}
                else if("/health".equals(path)) {
                    boolean ready=workers.stream().allMatch(ResortWorker::ready);status=ready?200:503;
                    body="{\"status\":\""+(ready?"UP":"DOWN")+"\"}";
                } else if("/stats".equals(path)) {
                    long count=0;for(StandingsStore s:stores)count+=s.processed();
                    body=new Gson().toJson(Map.of("uniqueSwipesProcessed",count,"resorts",10,
                            "processingPool",processing.stats(),"commitMetrics",metrics.stats()));
                } else if(path.matches("/leaderboard/[1-9][0-9]*/[1-9][0-9]*")) {
                    String[] p=path.split("/");int resort=Integer.parseInt(p[2]);
                    if(resort>10){status=404;body="{}";}else body=stores.get(resort-1).snapshot(p[3]);
                } else {status=404;body="{}";}
            } catch(Exception e){status=500;body="{\"error\":\"Unable to read state\"}";}
            byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type","application/json; charset=UTF-8");
            ex.sendResponseHeaders(status,bytes.length);try(java.io.OutputStream out=ex.getResponseBody()){out.write(bytes);}
        });
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            http.stop(0);httpThreads.shutdownNow();workers.forEach(ResortWorker::close);
            boolean interrupted=false;
            for(Thread t:threads)while(t.isAlive())try{t.join();}
            catch(InterruptedException e){interrupted=true;}
            if(interrupted)Thread.currentThread().interrupt();
            processing.close();
            for(StandingsStore s:stores)try{s.close();}catch(Exception ignored){}
        }));
        http.start();System.out.println("Consumer HTTP port "+Config.number("CONSUMER_HTTP_PORT",9090)+"; data: "+data);
    }
}
