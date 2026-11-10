package leaderboard;
import com.google.gson.*;
public final class Snapshot {
    public final String resort,day,json;
    public final long version;
    private Snapshot(String r,String d,long v,String j){resort=r;day=d;version=v;json=j;}
    public static Snapshot parse(String json) {
        JsonObject o=JsonParser.parseString(json).getAsJsonObject();
        String r=o.get("resortID").getAsString(),d=o.get("dayID").getAsString();
        long v=o.get("version").getAsLong();
        if(!positive(r)||Integer.parseInt(r)>10||!positive(d)||v<1||!o.get("top").isJsonArray())
            throw new IllegalArgumentException("Invalid leaderboard snapshot");
        java.time.Instant.parse(o.get("serverTimestamp").getAsString());
        return new Snapshot(r,d,v,json);
    }
    public static boolean positive(String s) {
        try{return s!=null && Integer.parseInt(s)>0 && Integer.toString(Integer.parseInt(s)).equals(s);}
        catch(NumberFormatException e){return false;}
    }
    public String key(){return resort+":"+day;}
}
