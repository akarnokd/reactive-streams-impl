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

import rx.Observable;
import rx.internal.util.*;
import rx.observables.BlockingObservable;
import rx.schedulers.Schedulers;

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
    
    @Param({ "64", "128", "256", "512", "1024" })
    public int bufferSize;
    
    Integer[] values;
    
    Publisher<Integer> source;
    Publisher<Integer> sourceIter;
    Publisher<Integer> sourceExec;
    BlockingObservable<Integer> blocking;
    BlockingObservable<Integer> blockingFJ;
    BlockingObservable<Integer> blockingExec;
    ExecutorService single;
    
    @Setup
    public void setup() {
        values = new Integer[times];
        Arrays.fill(values, 123);
        values[times - 1] = times;
        source = Publishers.observeOn(Publishers.fromArray(values), ForkJoinPool.commonPool(), bufferSize);
        sourceIter = Publishers.observeOn(Publishers.fromIterable(Arrays.asList(values)), ForkJoinPool.commonPool(), bufferSize);
        Observable<Integer> obs = Observable.from(Arrays.asList(values));
        blocking = obs.observeOn(Schedulers.computation()).toBlocking();
        blockingFJ = obs.observeOn(Schedulers.from(ForkJoinPool.commonPool())).toBlocking();
        single = Executors.newSingleThreadExecutor(new RxThreadFactory("RxSingleThreaded-"));
        sourceExec = Publishers.observeOn(Publishers.fromArray(values), single, bufferSize); 
        blockingExec = obs.observeOn(Schedulers.from(single)).toBlocking();
    }
    
    @Benchmark
    public Object observeOn() {
        return Publishers.getScalar(source);
    }
//    @Benchmark
    public Object observeOnExec() {
        return Publishers.getScalar(sourceExec);
    }
//    @Benchmark
    public Object observeOnIter() {
        return Publishers.getScalar(sourceIter);
    }
    
//    @Benchmark
    public Object rxObserveOn() {
        if (bufferSize != RxRingBuffer.SIZE) {
            throw new RuntimeException("Irrelevant buffer size");
        }
        return blocking.last();
    }

//    @Benchmark
    public Object rxObserveOnFJ() {
        if (bufferSize != RxRingBuffer.SIZE) {
            throw new RuntimeException("Irrelevant buffer size");
        }
        return blockingFJ.last();
    }
//    @Benchmark
    public Object rxObserveOnExec() {
        if (bufferSize != RxRingBuffer.SIZE) {
            throw new RuntimeException("Irrelevant buffer size");
        }
        return blockingExec.last();
    }
    
    public static void main(String[] args) {
        for (int i = 1; i <= 10_000_000; i *= 10) {
            for (int bs : new int[] { 64, 128, 256, 512, 1024 }) { 
                for (int j = 0; j < 100; j++) { 
                    System.out.print(i + "/" + bs + " ");
                    ObserveOnPerf perf = new ObserveOnPerf();
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
