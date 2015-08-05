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
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 * Benchmark ScalarJust.
 * <p>
 * gradlew jmh "-Pjmh=ScalarJustPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ScalarJustPerf {
    @Param({ "1", "1000", "1000000" })
    public int times;

    @Benchmark
    public void just(Blackhole bh) {
        int s = times;
        Publisher<Integer> source = Publishers.just(1);
        for (int i = 0; i < s; i++) {
            bh.consume(Publishers.getScalar(source));
        }
    }
    
    @Benchmark
    public void justFlatMapJust(Blackhole bh) {
        int s = times;
        Publisher<Integer> source = Publishers.just(1);
        Publisher<Integer> result = Publishers.flatMap(source, Publishers::just);
        for (int i = 0; i < s; i++) {
            bh.consume(Publishers.getScalar(result));
        }
    }
    @Benchmark
    public void justNow(Blackhole bh) {
        int s = times;
        Publisher<Integer> source = Publishers.just(1);
        for (int i = 0; i < s; i++) {
            bh.consume(Publishers.getScalarNow(source));
        }
    }
    
    @Benchmark
    public void justFlatMapJustNow(Blackhole bh) {
        int s = times;
        Publisher<Integer> source = Publishers.just(1);
        Publisher<Integer> result = Publishers.flatMap(source, Publishers::just);
        for (int i = 0; i < s; i++) {
            bh.consume(Publishers.getScalarNow(result));
        }
    }
}
