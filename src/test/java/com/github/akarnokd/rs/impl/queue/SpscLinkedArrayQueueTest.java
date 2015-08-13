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

package com.github.akarnokd.rs.impl.queue;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.akarnokd.rs.Publishers;

/**
 * 
 */
public class SpscLinkedArrayQueueTest {

    @Test
    public void offerPoll() {
        for (int n = 1; n <= Publishers.bufferSize(); n *= 2) {
            SpscLinkedArrayQueue<Integer> queue = new SpscLinkedArrayQueue<>(n);
            
            for (int i = 0; i < n; i++) {
                assertTrue(queue.offer(i));
                assertFalse(queue.isEmpty());
                assertEquals((Integer)i, queue.peek());
                assertEquals((Integer)i, queue.poll());
            }
            
            for (int i = 0; i < n; i++) {
                assertTrue(queue.offer(i));
            }
            for (int i = 0; i < n; i++) {
                assertEquals((Integer)i, queue.peek());
                assertEquals((Integer)i, queue.poll());
            }
        }
    }
}
