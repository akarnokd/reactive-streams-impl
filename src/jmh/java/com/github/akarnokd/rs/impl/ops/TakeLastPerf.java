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
 * TakeLast performance.
 * <p>
 * gradlew jmh "-Pjmh=TakeLastPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class TakeLastPerf {
    @Param({"1", "1000", "1000000"})
    public int count;
    
    Publisher<Integer> lastN;
    Publisher<Integer> last1;
    Publisher<Integer> lastN1;
    
    Observable<Integer> rxLastN;
    Observable<Integer> rxLast1;
    
    @Setup
    public void setup() {
        Publisher<Integer> source = Publishers.range(1, count);
        lastN = Publishers.takeLast(source, Math.max(count / 2, 1));
        last1 = Publishers.takeLast(source, 1);
        lastN1 = new TakeLast<>(source, 1);
        
        Observable<Integer> rxSource = Observable.range(1, count);
        
        rxLastN = rxSource.takeLast(Math.max(count / 2, 1));
        rxLast1 = rxSource.takeLast(1);
    }
    
    @Benchmark
    public Object lastN() {
        return Publishers.getScalarNow(lastN);
    }
    @Benchmark
    public Object last1() {
        return Publishers.getScalarNow(last1);
    }
    @Benchmark
    public Object lastN1() {
        return Publishers.getScalarNow(lastN1);
    }
    @Benchmark
    public Object rxlastN() {
        return rxLastN.subscribe();
    }
    @Benchmark
    public Object rxlast1() {
        return rxLast1.subscribe();
    }
}
