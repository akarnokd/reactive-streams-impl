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
package com.github.akarnokd.rs;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.junit.*;
import org.reactivestreams.Publisher;

/**
 * Have one or two tests of major {@link Publishers} operators.
 */
public class PublishersTest {
    @Test
    public void just() {
        Publisher<Integer> source = Publishers.just(1);
        
        Integer value = Publishers.getScalar(source);
        
        Assert.assertEquals((Integer)1, value);
    }
    
    @Test
    public void scalarFlatMapScalar() {
        Publisher<Integer> source = Publishers.just(1);
        
        Publisher<Integer> result = Publishers.flatMap(source, Publishers::just);

        Integer value = Publishers.getScalar(result);
        
        Assert.assertEquals((Integer)1, value);
    }
    
    @Test
    public void arraySource() {
        Publisher<Integer> source = Publishers.fromArray(1, 2, 3, 4);
        List<Integer> result = Publishers.getList(source);
        
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), result);
    }
    
    @Test
    public void take() {
        Publisher<Integer> source = Publishers.fromArray(1, 2, 3, 4);
        List<Integer> result = Publishers.getList(Publishers.take(source, 2));
        Assert.assertEquals(Arrays.asList(1, 2), result);
    }
    
    @Test
    public void fromStream() {
        Publisher<Integer> source = Publishers.fromStream(java.util.stream.Stream.of(1, 2, 3, 4));
        List<Integer> result = Publishers.getListNow(source);
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), result);
    }
    
    @Test
    public void fromStreamAndTake() {
        Stream<Integer> s = java.util.stream.Stream.of(1, 2, 3, 4);
        Publisher<Integer> source = Publishers.fromStream(s);
        List<Integer> result = Publishers.getListNow(Publishers.take(source, 2));
        Assert.assertEquals(Arrays.asList(1, 2), result);
    }
    
    @Test(expected = IllegalStateException.class)
    public void fromStreamExhausted() {
        Stream<Integer> s = java.util.stream.Stream.of(1, 2, 3, 4);
        s.count();
        Publisher<Integer> source = Publishers.fromStream(s);
        List<Integer> result = Publishers.getListNow(source);
        Assert.assertEquals(Collections.emptyList(), result);
        
    }
    @Test(expected = IllegalStateException.class)
    public void fromStreamClosed() {
        Publisher<Integer> source;
        try (Stream<Integer> s = java.util.stream.Stream.of(1, 2, 3, 4)) {
            source = Publishers.fromStream(s);
        }
        List<Integer> result = Publishers.getListNow(source);
        Assert.assertEquals(Collections.emptyList(), result);
        
    }
    
    @Test
    public void observeOn() {
        Publisher<Integer> source = Publishers.fromArray(1, 2, 3, 4, 5);
        Publisher<Integer> result = Publishers.observeOn(source, ForkJoinPool.commonPool());

        List<Integer> list = Publishers.getList(result);
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
    }
}
