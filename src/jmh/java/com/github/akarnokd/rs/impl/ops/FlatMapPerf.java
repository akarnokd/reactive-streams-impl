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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

import rx.Observable;

/**
 * Example performance class.
 * <p>
 * gradlew jmh "-Pjmh=FlatMapPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class FlatMapPerf {
    @Param({ "1", "1000", "1000000" })
    public int times;

    Publisher<Integer> source;
    Publisher<Integer> source2;
    
    Observable<Integer> rxSource;
    Observable<Integer> rxSource2;
    
    @Setup
    public void setup() {
        Publisher<Integer> range = Publishers.range(0, times);
        source = Publishers.flatMap(range, Publishers::just);
        source2 = Publishers.flatMap(range, v -> Publishers.range(v, 2));
        
        Observable<Integer> rxRange = Observable.range(0, times);
        rxSource = rxRange.flatMap(Observable::just);
        rxSource2 = rxRange.flatMap(v -> Observable.range(v, 2));
    }
    
    @Benchmark
    public Object flatMap() {
        return Publishers.getScalarNow(source);
    }
    @Benchmark
    public Object flatMap2() {
        return Publishers.getScalarNow(source2);
    }
    @Benchmark
    public Object rxFlatMap() {
        return rxSource.subscribe();
    }
    @Benchmark
    public Object rxFlatMap2() {
        return rxSource2.subscribe();
    }
}
