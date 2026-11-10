package messaging;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
public final class LiftVertical {
    private final int[] feet = new int[41];
    public LiftVertical() {
        try (InputStream in = LiftVertical.class.getResourceAsStream("/lift_vertical.json")) {
            if (in == null) throw new IllegalStateException("Missing lift_vertical.json");
            JsonObject lifts = JsonParser.parseReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                    .getAsJsonObject().getAsJsonObject("lifts");
            if (lifts.size() != 40) throw new IllegalStateException("Expected 40 lifts");
            for (int i = 1; i <= 40; i++) {
                feet[i] = lifts.get(Integer.toString(i)).getAsInt();
                if (feet[i] < 60 || feet[i] > 400) throw new IllegalStateException("Invalid vertical");
            }
        } catch (Exception e) { throw new IllegalStateException("Cannot load lift table", e); }
    }
    public int get(int lift) {
        if (lift < 1 || lift > 40) throw new IllegalArgumentException("liftID must be 1-40");
        return feet[lift];
    }
}
