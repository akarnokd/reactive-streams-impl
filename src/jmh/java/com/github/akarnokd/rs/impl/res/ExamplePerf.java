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

package com.github.akarnokd.rs.impl.res;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

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
public class ExamplePerf {
    @Param({ "1", "1000", "1000000" })
    public int times;

    @Param({ "1", "10", "100", "1000", "10000", "100000", "1000000" })
    public int bufferSize;

    IndexedResourceContainer<Integer> irc;
    
    @Setup
    public void setup() {
        irc = new IndexedResourceContainer<>(bufferSize, e -> { });
    }
    
    int i;
    
    @Benchmark
    public void addRemove() {
        if (times != 1) {
            throw new RuntimeException("Irrelevant benchmark settings");
        }
        int i = this.i;
        IndexedResourceContainer<Integer> irc = this.irc;
        irc.allocate(i);
        irc.setResource(i, 1);
        irc.deleteResource(i);
        this.i = i + 1;
    }
    
    @Benchmark
    public void addAllRemoveAll() {
        int s = times;
        int i = this.i;
        IndexedResourceContainer<Integer> irc = this.irc;
        for (int j = 0; j < s; j++) {
            irc.allocate(i + j);
            irc.setResource(i + j, 1);
        }
        for (int j = 0; j < s; j++) {
            irc.deleteResource(i + j);
        }
        this.i = i + s;
    }
    
}
