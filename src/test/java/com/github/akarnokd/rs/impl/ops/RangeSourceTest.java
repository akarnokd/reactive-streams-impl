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

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * 
 */
public class RangeSourceTest {
    @Test
    public void deliveryNoBackpressure() {
        for (int i = 1; i <= 1_000_000; i *= 10) {
            Publisher<Integer> source = Publishers.range(0, i);
            
            int last = Publishers.getScalar(source);
            
            Assert.assertEquals(last, i - 1);
        }
    }
    @Test
    public void deliveryWithBackpressure() {
        for (int i = 1; i <= 1_000_000; i *= 10) {
            Publisher<Integer> source = Publishers.range(0, i);
            
            int last = Publishers.getScalar(source, i > 1 ? i >> 2 : 1);
            
            Assert.assertEquals(last, i - 1);
        }
    }
}
