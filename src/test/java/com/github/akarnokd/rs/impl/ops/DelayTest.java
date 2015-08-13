/*
 * Copyright 2011-2015 David Karnok
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.akarnokd.rs.impl.ops;

import java.util.concurrent.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * 
 */
public class DelayTest {
    @Test
    public void delay() {
        for (int i = 1; i < 4; i++) {
            System.out.printf("Delay, concurrency: %d%n", i);
            ScheduledExecutorService exec = Executors.newScheduledThreadPool(i);
            try {
                for (int j = 1; j <= 1_000_000; j *= 10) {
                    System.out.printf("  elements: %d%n", j);
                    for (int k = 1; k <= 1_000_000; k *= 10) {
                        System.out.printf("    bufferSize: %d%n", k);
                        
                        Publisher<Integer> source = Publishers.range(1, j);
                        Publisher<Integer> result = Publishers.delay(source, 100, TimeUnit.MILLISECONDS, exec, k);
                        
                        Assert.assertEquals((Integer)j, Publishers.getScalar(result));
                    }
                }
            } finally {
                exec.shutdownNow();
            }
        }
    }
    
    @Test
    public void oneBufferMillionItems() {
        
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        
        try {
            Publisher<Integer> source = Publishers.range(1, 1_000_000);
            Publisher<Integer> result = Publishers.delay(source, 100, TimeUnit.MILLISECONDS, exec, 1);
            
            Assert.assertEquals((Integer)1_000_000, Publishers.getScalar(result));
        } finally {
            exec.shutdownNow();
        }

    }
}
