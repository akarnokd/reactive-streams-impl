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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

import rx.Observable;
import rx.observables.BlockingObservable;

/**
 * Benchmark observeOn.
 * <p>
 * gradlew jmh "-Pjmh=FilterPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class FilterPerf {
    @Param({ "1", "1000", "1000000" })
    public int times;
    
//    @Param({ "64", "128", "256", "512", "1024" })
//    public int bufferSize;
    
    Integer[] values;
    
    Publisher<Integer> source;
    Publisher<Integer> source3;
    BlockingObservable<Integer> blocking;
    BlockingObservable<Integer> blocking3;
    
    @Setup
    public void setup() {
        values = new Integer[times];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        source = Publishers.filter(Publishers.fromArray(values), v -> (v & 1) == 0);
        source3 = Publishers.filter(source, v -> (v & 3) == 0);
        source3 = Publishers.filter(source3, v -> (v & 7) == 0);
        
        Observable<Integer> obs = Observable.from(Arrays.asList(values));
        Observable<Integer> obs2 = obs.filter(v -> (v & 1) == 0);
        
        blocking = obs2.toBlocking();
        blocking3 = obs2.filter(v -> (v & 3) == 0)
                .filter(v -> (v & 7) == 0)
                .toBlocking();
    }
    
    @Benchmark
    public Object filter() {
        return Publishers.getScalar(source);
    }
    @Benchmark
    public Object rxFilter() {
        return blocking.last();
    }
    @Benchmark
    public Object filter3() {
        return Publishers.getScalar(source3);
    }
    @Benchmark
    public Object rxFilter3() {
        return blocking3.last();
    }
    
    public static void main(String[] args) {
        FilterPerf p = new FilterPerf();
        p.times = 1;
        p.setup();
        p.filter();
    }
}
