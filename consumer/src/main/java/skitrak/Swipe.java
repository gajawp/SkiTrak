package skitrak;
import java.util.UUID;
import java.time.Instant;
public final class Swipe {
    public String eventId,resortID,dayID,skierID,liftID,timestamp,serverId;
    public int vertical,time;
    public void validate(int resort) {
        if(eventId==null || !UUID.fromString(eventId).toString().equalsIgnoreCase(eventId))
            throw new IllegalArgumentException("Invalid eventId");
        eventId=eventId.toLowerCase(java.util.Locale.ROOT);
        if(!Integer.toString(resort).equals(resortID))throw new IllegalArgumentException("Wrong resort queue");
        if(!positive(dayID)||!positive(skierID)||!positive(liftID)||Integer.parseInt(liftID)>40
                ||vertical<60||vertical>400||time<1||time>360)
            throw new IllegalArgumentException("Invalid swipe fields");
        Instant.parse(timestamp);
    }
    private static boolean positive(String s) {
        try{return s!=null && Integer.parseInt(s)>0 && Integer.toString(Integer.parseInt(s)).equals(s);}
        catch(NumberFormatException e){return false;}
    }
    // Identity excludes timestamp/server: an HTTP retry can arrive on another ingest host.
    public String fingerprint(){return resortID+":"+dayID+":"+skierID+":"+liftID+":"+vertical+":"+time;}
}
