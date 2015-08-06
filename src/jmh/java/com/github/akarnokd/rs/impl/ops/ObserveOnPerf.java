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
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * Benchmark observeOn.
 * <p>
 * gradlew jmh "-Pjmh=ObserveOnPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ObserveOnPerf {
    @Param({ "1", "10", "100", "1000", "10000", "100000", "1000000" })
    public int times;
    
    Integer[] values;
    
    Publisher<Integer> source;
    
    @Setup
    public void setup() {
        values = new Integer[times];
        Arrays.fill(values, 123);
        values[times - 1] = times;
        source = Publishers.observeOn(Publishers.fromArray(values), ForkJoinPool.commonPool());
    }
    
    @Benchmark
    public Object observeOn() {
        ScalarSubscriber<Integer> s = new ScalarSubscriber<>();
        source.subscribe(s);
        return s.getValue();
    }
    
    public static void main(String[] args) {
        for (int i = 1; i <= 1000_0000; i *= 10) {
            System.out.print(i);
            ObserveOnPerf perf = new ObserveOnPerf();
            perf.times = i;
            perf.setup();
            if ((Integer)perf.observeOn() == i) {
                System.out.println("-ok");
            } else {
                System.out.println("-err");
            }
        }
    }
}
