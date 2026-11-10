package skitrak;
import com.google.gson.Gson;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** One durable DB per resort; aggregation, deduplication and outbox commit together. */
public final class StandingsStore implements AutoCloseable {
    private final Connection db;
    private final int resort,topN;
    private final Gson gson=new Gson();
    private final CommitMetrics metrics;
    // A read sees one immutable, successfully committed state without waiting for SQL.
    private static final class CommittedState {
        final long count;
        final Map<String,String> snapshots;
        CommittedState(long count, Map<String,String> snapshots) {
            this.count=count;
            this.snapshots=Collections.unmodifiableMap(new HashMap<>(snapshots));
        }
    }
    private volatile CommittedState committed;
    public StandingsStore(String jdbcUrl,int resort,int topN) throws SQLException {
        this(jdbcUrl,resort,topN,new CommitMetrics());
    }
    public StandingsStore(String jdbcUrl,int resort,int topN,CommitMetrics metrics) throws SQLException {
        this.metrics=Objects.requireNonNull(metrics);
        this.resort=resort;this.topN=topN;
        db=DriverManager.getConnection(jdbcUrl,"sa","");
        try(Statement s=db.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS processed (id VARCHAR(36) PRIMARY KEY, fingerprint VARCHAR(256) NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS totals (day_id VARCHAR(16), skier VARCHAR(16), feet BIGINT NOT NULL, PRIMARY KEY(day_id,skier))");
            // Idempotent migration: H2 fills the generated value for existing rows.
            // Inserts and updates maintain it automatically.
            s.execute("ALTER TABLE totals ADD COLUMN IF NOT EXISTS skier_numeric BIGINT GENERATED ALWAYS AS (CAST(skier AS BIGINT))");
            s.execute("CREATE INDEX IF NOT EXISTS ranking_numeric_v2 ON totals(day_id,feet DESC,skier_numeric ASC)");
            // Retire the text-order index only after its replacement exists.
            s.execute("DROP INDEX IF EXISTS ranking");
            s.execute("CREATE TABLE IF NOT EXISTS snapshots (day_id VARCHAR(16) PRIMARY KEY, version BIGINT NOT NULL, payload CLOB NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS outbox (seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, payload CLOB NOT NULL)");
            long count;
            try(ResultSet rs=s.executeQuery("SELECT COUNT(*) FROM processed")) {rs.next();count=rs.getLong(1);}
            Map<String,String> snapshots=new HashMap<>();
            try(ResultSet rs=s.executeQuery("SELECT day_id,payload FROM snapshots")) {
                while(rs.next())snapshots.put(rs.getString(1),rs.getString(2));
            }
            committed=new CommittedState(count,snapshots);
        } catch (SQLException | RuntimeException error) {
            try { db.close(); } catch (SQLException closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
    }
    public synchronized boolean apply(Swipe event) throws SQLException {
        return applyBatch(Collections.singletonList(event)).get(0);
    }

    /** Atomically applies deliveries in queue order; collisions roll back the batch. */
    public synchronized List<Boolean> applyBatch(List<Swipe> events) throws SQLException {
        long startedNanos=System.nanoTime();
        List<Boolean> applied=new ArrayList<>(events.size());
        if(events.isEmpty())return applied;
        // Canonicalize IDs before lookup, just as the previous per-event path did.
        for(Swipe event:events)event.validate(resort);
        Instant[] freshSources=new Instant[events.size()];
        int freshCount=0;
        Instant committedAt;
        long elapsedNanos;
        db.setAutoCommit(false);
        try {
            Map<String,String> known=existingFingerprints(events);
            Map<String,String> before=new LinkedHashMap<>(),lastTimestamp=new HashMap<>();
            Map<String,Map<String,Long>> deltas=new LinkedHashMap<>();
            Map<String,String> fresh=new LinkedHashMap<>();
            for(Swipe event:events) {
                String fingerprint=event.fingerprint();
                String previous=known.get(event.eventId);
                if(previous!=null) {
                    if(!previous.equals(fingerprint))throw new IllegalArgumentException("eventId reused for different swipe");
                    applied.add(false);
                    continue;
                }
                known.put(event.eventId,fingerprint);
                fresh.put(event.eventId,fingerprint);
                applied.add(true);
                freshSources[freshCount++]=Instant.parse(event.timestamp);
                if(!before.containsKey(event.dayID))before.put(event.dayID,gson.toJson(top(event.dayID)));
                // Positive integer verticals are additive. Deduplication still follows delivery order.
                deltas.computeIfAbsent(event.dayID,k->new LinkedHashMap<>())
                        .merge(event.skierID,(long)event.vertical,Math::addExact);
                lastTimestamp.put(event.dayID,event.timestamp);
            }
            try(PreparedStatement insert=db.prepareStatement("INSERT INTO processed(id,fingerprint) VALUES(?,?)")) {
                for(Map.Entry<String,String> entry:fresh.entrySet()) {
                    insert.setString(1,entry.getKey());insert.setString(2,entry.getValue());insert.addBatch();
                }
                if(!fresh.isEmpty())insert.executeBatch();
            }
            try(PreparedStatement update=db.prepareStatement("UPDATE totals SET feet=feet+? WHERE day_id=? AND skier=?");
                PreparedStatement insert=db.prepareStatement("INSERT INTO totals(day_id,skier,feet) VALUES(?,?,?)")) {
                for(Map.Entry<String,Map<String,Long>> day:deltas.entrySet()) {
                    for(Map.Entry<String,Long> skier:day.getValue().entrySet()) {
                        update.setLong(1,skier.getValue());update.setString(2,day.getKey());update.setString(3,skier.getKey());
                        if(update.executeUpdate()==0) {
                            insert.setString(1,day.getKey());insert.setString(2,skier.getKey());insert.setLong(3,skier.getValue());
                            insert.executeUpdate();
                        }
                    }
                }
            }
            Map<String,String> snapshots=new HashMap<>(committed.snapshots);
            for(Map.Entry<String,String> day:before.entrySet()) {
                String payload=publishIfChanged(day.getKey(),day.getValue(),lastTimestamp.get(day.getKey()));
                if(payload!=null)snapshots.put(day.getKey(),payload);
            }
            CommittedState next=new CommittedState(Math.addExact(committed.count,fresh.size()),snapshots);
            db.commit();
            elapsedNanos=System.nanoTime()-startedNanos;
            committedAt=Instant.now();
            committed=next;
        } catch(SQLException | RuntimeException error) {
            try {db.rollback();}catch(SQLException rollbackError){error.addSuppressed(rollbackError);}
            throw error;
        } finally {db.setAutoCommit(true);}
        metrics.record(freshSources,freshCount,committedAt,elapsedNanos);
        return applied;
    }

    // Bounded primary-key lookups avoid a separate SELECT/ResultSet for every delivery.
    private Map<String,String> existingFingerprints(List<Swipe> events) throws SQLException {
        List<String> ids=new ArrayList<>(new LinkedHashSet<>(eventIds(events)));
        Map<String,String> known=new HashMap<>();
        for(int start=0;start<ids.size();start+=1000) {
            int end=Math.min(start+1000,ids.size());
            String parameters=String.join(",",Collections.nCopies(end-start,"?"));
            try(PreparedStatement query=db.prepareStatement("SELECT id,fingerprint FROM processed WHERE id IN ("+parameters+")")) {
                for(int i=start;i<end;i++)query.setString(i-start+1,ids.get(i));
                try(ResultSet rs=query.executeQuery()) {
                    while(rs.next())known.put(rs.getString(1),rs.getString(2));
                }
            }
        }
        return known;
    }
    private static List<String> eventIds(List<Swipe> events) {
        List<String> ids=new ArrayList<>(events.size());
        for(Swipe event:events)ids.add(event.eventId);
        return ids;
    }
    // Caller owns the transaction and the store monitor.
    private String publishIfChanged(String dayID, String before, String sourceTimestamp) throws SQLException {
        List<Map<String,Object>> after=top(dayID);
        if(before.equals(gson.toJson(after)))return null;
        long version=1;
        try(PreparedStatement q=db.prepareStatement("SELECT version FROM snapshots WHERE day_id=?")) {
            q.setString(1,dayID);try(ResultSet rs=q.executeQuery()){if(rs.next())version=rs.getLong(1)+1;}
        }
        Map<String,Object> snapshot=new LinkedHashMap<>();
        snapshot.put("resortID",Integer.toString(resort));snapshot.put("dayID",dayID);
        snapshot.put("version",version);snapshot.put("serverTimestamp",Instant.now().toString());
        snapshot.put("sourceTimestamp",sourceTimestamp);snapshot.put("top",after);
        String payload=gson.toJson(snapshot);
        try(PreparedStatement q=db.prepareStatement("MERGE INTO snapshots(day_id,version,payload) KEY(day_id) VALUES(?,?,?)")) {
            q.setString(1,dayID);q.setLong(2,version);q.setString(3,payload);q.executeUpdate();
        }
        try(PreparedStatement q=db.prepareStatement("INSERT INTO outbox(payload) VALUES(?)")) {
            q.setString(1,payload);q.executeUpdate();
        }
        return payload;
    }
    private List<Map<String,Object>> top(String day) throws SQLException {
        List<Map<String,Object>> rows=new ArrayList<>();
        try(PreparedStatement q=db.prepareStatement("SELECT skier,feet FROM totals USE INDEX (ranking_numeric_v2) WHERE day_id=? ORDER BY day_id ASC,feet DESC,skier_numeric ASC LIMIT ?")) {
            q.setString(1,day);q.setInt(2,topN);
            try(ResultSet rs=q.executeQuery()) {
                while(rs.next()) {
                    Map<String,Object> row=new LinkedHashMap<>();row.put("rank",rows.size()+1);
                    row.put("skierID",rs.getString(1));row.put("vertical",rs.getLong(2));rows.add(row);
                }
            }
        }return rows;
    }
    public String snapshot(String day) throws SQLException {
        return committed.snapshots.getOrDefault(day,"{}");
    }
    public synchronized long total(String day,String skier) throws SQLException {
        try(PreparedStatement q=db.prepareStatement("SELECT feet FROM totals WHERE day_id=? AND skier=?")) {
            q.setString(1,day);q.setString(2,skier);try(ResultSet rs=q.executeQuery()){return rs.next()?rs.getLong(1):0;}
        }
    }
    public long processed() throws SQLException {return committed.count;}
    public synchronized Map.Entry<Long,String> nextUpdate() throws SQLException {
        try(Statement s=db.createStatement();ResultSet rs=s.executeQuery("SELECT seq,payload FROM outbox ORDER BY seq LIMIT 1")) {
            return rs.next()?new AbstractMap.SimpleImmutableEntry<>(rs.getLong(1),rs.getString(2)):null;
        }
    }
    public synchronized void published(long seq) throws SQLException {
        try(PreparedStatement q=db.prepareStatement("DELETE FROM outbox WHERE seq=?")){q.setLong(1,seq);q.executeUpdate();}
    }
    @Override public synchronized void close() throws SQLException {db.close();}
}
