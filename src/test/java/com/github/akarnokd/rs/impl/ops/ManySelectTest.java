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

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 *
 */
public class ManySelectTest {
    @Test(timeout = 60000)
    public void simple() {
        for (int j = 0; j < 50; j++) {
            System.out.println("simple ==> " +j);
            ExecutorService exec = Executors.newSingleThreadExecutor();
            try {
                for (int i = 0; i < 10000; i++) {
                    if (i % 10 == 0) {
                        System.out.println("   --> " + i);
                    }
                    simpleBody(exec);
                }
            } finally {
                exec.shutdownNow();
            }
        }
    }
    
    @Test(timeout = 60000)
    public void simpleSynchronous() {
        for (int j = 0; j < 50; j++) {
            System.out.println("simple ==> " +j);
            CurrentExecutor exec = new CurrentExecutor();
            try {
                for (int i = 0; i < 10000; i++) {
                    if (i % 100 == 0) {
                        System.out.println("   --> " + i);
                    }
                    simpleBody(exec);
                }
            } finally {
                exec.shutdownNow();
            }
        }
    }
    
    static final class CurrentExecutor implements ExecutorService {

        @Override
        public void execute(Runnable command) {
            
        }

        @Override
        public void shutdown() {
            
        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task) {
            task.run();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
        
    }

    static final Set<Integer> simpleSet = new HashSet<>(Arrays.asList(55, 54, 52, 49, 45, 40, 34, 27, 19, 10));
    
    private void simpleBody(ExecutorService exec) {
        Publisher<Integer> source = Publishers.range(1, 10);
        
        Publisher<Integer> result = Publishers.merge(
            Publishers.manySelect(source, p -> Publishers.sumInteger(p), exec)
        );
        
        Set<Integer> list = new HashSet<>(Publishers.getList(result));
        Assert.assertEquals(simpleSet, list);
    }
}
