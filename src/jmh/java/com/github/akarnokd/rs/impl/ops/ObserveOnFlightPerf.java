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
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * Benchmark observeOn.
 * <p>
 * gradlew jmh "-Pjmh=ObserveOnFlightPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ObserveOnFlightPerf {
    @Param({ "1", "1000", "1000000" })
    public int times;
    
    @Param({ "128" })
    public int bufferSize;
    
    Publisher<Integer> source;
    
    @Setup
    public void setup() {
        Integer[] values = new Integer[times];
        Arrays.fill(values, 123);
        values[times - 1] = times;
        source = Publishers.observeOn(Publishers.fromArray(values), ForkJoinPool.commonPool(), bufferSize);
    }
    
    @Benchmark
    public Object observeOn() {
        return Publishers.getScalar(source);
    }
    
    public static void main(String[] args) {
        for (int i = 1; i <= 10_000_000; i *= 10) {
            for (int bs : new int[] { 64, 128, 256, 512, 1024 }) { 
                for (int j = 0; j < 100; j++) { 
                    System.out.print(i + "/" + bs + " ");
                    ObserveOnFlightPerf perf = new ObserveOnFlightPerf();
                    perf.times = i;
                    perf.bufferSize = bs;
                    perf.setup();
                    if (Objects.equals(perf.observeOn(), i)) {
                        System.out.println("-ok");
                    } else {
                        System.err.println("-err");
                    }
                }
            }
        }
    }
}
