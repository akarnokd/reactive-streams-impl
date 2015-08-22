package com.github.akarnokd.rs.impl.ops;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

public class BufferTimedTest {
    @Test
    public void simple() {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            Publisher<Long> source = Publishers.periodicTimer(100, 200, TimeUnit.MILLISECONDS, exec);
            
            Publisher<List<Long>> result = Publishers.take(Publishers.bufferTimed(source, 1, TimeUnit.SECONDS, exec), 5);
            
            List<List<Long>> list = Publishers.getList(result);
            
            Assert.assertEquals(Arrays.asList(
                    Arrays.asList(0L, 1L, 2L, 3L, 4L),
                    Arrays.asList(5L, 6L, 7L, 8L, 9L),
                    Arrays.asList(10L, 11L, 12L, 13L, 14L),
                    Arrays.asList(15L, 16L, 17L, 18L, 19L),
                    Arrays.asList(20L, 21L, 22L, 23L, 24L)
            ), list);
            
        } finally {
            exec.shutdownNow();
        }
    }
}
