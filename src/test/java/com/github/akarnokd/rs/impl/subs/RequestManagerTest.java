package com.github.akarnokd.rs.impl.subs;
import org.junit.Test;
import static org.junit.Assert.*;

public class RequestManagerTest {
    @Test
    public void testAddCap() {
        assertEquals(2L, RequestManager.addCap(1, 1));
        assertEquals(Long.MAX_VALUE, RequestManager.addCap(1, Long.MAX_VALUE - 1));
        assertEquals(Long.MAX_VALUE, RequestManager.addCap(1, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, RequestManager.addCap(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1));
        assertEquals(Long.MAX_VALUE, RequestManager.addCap(Long.MAX_VALUE, Long.MAX_VALUE));
    }
    
    @Test
    public void testMultiplyCap() {
        assertEquals(6, RequestManager.multiplyCap(2, 3));
        assertEquals(Long.MAX_VALUE, RequestManager.multiplyCap(2, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, RequestManager.multiplyCap(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, RequestManager.multiplyCap(1L << 32, 1L << 32));

    }
}
