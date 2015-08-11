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
import rx.observables.BlockingObservable;

/**
 * Example performance class.
 * <p>
 * gradlew jmh "-Pjmh=ExamplePerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ConcatMapPerf {
    @Param({ "1", "1000", "1000000" })
    public int times;

    Publisher<Integer> source;
    
    BlockingObservable<Integer> rxSource;
    
    @Setup
    public void setup() {
        source = Publishers.concatMap(Publishers.range(0, times), Publishers::just);
        
        rxSource = Observable.range(0, times).concatMap(Observable::just).toBlocking();
    }
    
    @Benchmark
    public Object concatMap() {
        return Publishers.getScalar(source);
    }
    @Benchmark
    public Object rxConcatMap() {
        return rxSource.last();
    }
}
