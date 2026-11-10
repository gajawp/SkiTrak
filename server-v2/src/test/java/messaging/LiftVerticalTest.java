package messaging;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class LiftVerticalTest {
    @Test void loadsProvidedTableAndComputesKnownTotal() {
        LiftVertical t=new LiftVertical();
        assertEquals(167,t.get(1)); assertEquals(74,t.get(2));
        assertEquals(241,t.get(1)+t.get(2)); assertEquals(269,t.get(40));
        for(int i=1;i<=40;i++) assertTrue(t.get(i)>=60 && t.get(i)<=400);
        assertThrows(IllegalArgumentException.class, () -> t.get(0));
        assertThrows(IllegalArgumentException.class, () -> t.get(41));
    }
}
