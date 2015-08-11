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

import java.util.*;
import java.util.concurrent.ForkJoinPool;

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * 
 */
public class ConcatMapTest {
    
    @Test
    public void concatScalars() {
        Publisher<Integer> source = Publishers.range(1, 3);
        
        Publisher<Integer> result = Publishers.concatMap(source, Publishers::just);
        
        List<Integer> list = Publishers.getList(result);
        
        Assert.assertEquals(Arrays.asList(1, 2, 3), list);
    }
    
    @Test
    public void concatScalars2() {
        for (int i = 1; i <= 1_000_000; i *= 10) {
            Publisher<Integer> source = Publishers.range(1, i);
        
            Publisher<Integer> result = Publishers.concatMap(source, Publishers::just);
            
            Integer v = Publishers.getScalar(result);
            
            Assert.assertEquals((Integer)i, v);
        }
    }
    
    @Test
    public void concatScalarsAsync() {
        for (int i = 1; i <= 1_000_000; i *= 10) {
            Publisher<Integer> source = Publishers.range(1, i);
            
            Publisher<Integer> result = Publishers.concatMap(source, v -> 
                Publishers.<Integer>observeOn(Publishers.just(v), ForkJoinPool.commonPool())
            );
            
            Integer v = Publishers.getScalar(result);
            
            Assert.assertEquals((Integer)i, v);
        }
    }
}
