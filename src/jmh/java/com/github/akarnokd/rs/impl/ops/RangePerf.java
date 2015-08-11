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
public class RangePerf {
    @Param({ "1", "1000", "1000000" })
    public int times;

    Publisher<Integer> range;
    Publisher<Integer> rangeRaw;
    
    Publisher<Integer> rangeFilter;
    Publisher<Integer> rangeNoFuseFilter;
    
    @Setup
    public void setup() {
        range = Publishers.range(0, times);
        rangeRaw = new RangeSource(0, times);
        rangeFilter = Publishers.filter(range, v -> (v & 1) == 0);
        rangeNoFuseFilter = Publishers.filter(Publishers.asPublisher(range), v -> (v & 1) == 0); 
    }
    
    @Benchmark
    public Object range() {
        return Publishers.getScalar(range);
    }
    @Benchmark
    public Object rangeRaw() {
        return Publishers.getScalar(rangeRaw);
    }
    @Benchmark
    public Object rangeFilter() {
        return Publishers.getScalar(rangeFilter);
    }
    @Benchmark
    public Object rangeNoFuseFilter() {
        return Publishers.getScalar(rangeNoFuseFilter);
    }
}
