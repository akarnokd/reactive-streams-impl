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

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.queue.SpscLinkedArrayQueue;
import com.github.akarnokd.rs.impl.res.IndexedResourceContainer;
import com.github.akarnokd.rs.impl.subs.EmptySubscription;

/**
 * 
 */
public final class Delay<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final long delay;
    final TimeUnit unit;
    final Supplier<? extends ScheduledExecutorService> schedulerSupplier;
    final int bufferSize;

    public Delay(Publisher<? extends T> source, long delay, TimeUnit unit, Supplier<? extends ScheduledExecutorService> schedulerSupplier, int bufferSize) {
        this.source = source;
        this.delay = delay;
        this.unit = unit;
        this.schedulerSupplier = schedulerSupplier;
        this.bufferSize = bufferSize;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        ScheduledExecutorService exec;
        try {
            exec = schedulerSupplier.get();
        } catch (Throwable e) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(e);
            return;
        }
        if (exec == null) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(new NullPointerException());
            return;
        }
        
        source.subscribe(new DelaySubscriber<>(s, delay, unit, exec, bufferSize));
    }
    
    static final class DelaySubscriber<T> extends AtomicInteger 
    implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = 4636440010498003850L;
        final Subscriber<? super T> actual;
        final long delay;
        final TimeUnit unit;
        final ScheduledExecutorService scheduler;
        final Queue<T> queue;
        
        final IndexedResourceContainer<Future<?>> futures;
        
        
        volatile boolean done;
        Throwable error;
        
        /** How many events have been received. */
        long producerIndex;
        
        /** How many elements have reached their delay time. */
        volatile long targetIndex;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<DelaySubscriber> TARGET_INDEX =
                AtomicLongFieldUpdater.newUpdater(DelaySubscriber.class, "targetIndex");
        
        /** How many elements were emitted. */
        long consumerIndex;

        Subscription s;
        volatile boolean cancelled;
        
        public DelaySubscriber(Subscriber<? super T> actual, long delay, TimeUnit unit,
                ScheduledExecutorService scheduler, int bufferSize) {
            this.actual = actual;
            this.delay = delay;
            this.unit = unit;
            this.scheduler = scheduler;
            this.queue = new SpscLinkedArrayQueue<>(bufferSize);
            this.futures = new IndexedResourceContainer<>(bufferSize, f -> f.cancel(true));
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already received!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(T t) {
            long index = producerIndex++;
            queue.offer(t);
            final IndexedResourceContainer<Future<?>> futures = this.futures;

            if (!futures.allocate(index)) {
                return;
            }
            Future<?> f = scheduler.schedule(() -> {
                futures.deleteResource(index);
                emit(index + 1);
            }, delay, unit);
            
            futures.setResource(index, f);
        }
        
        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            long index = producerIndex++;
            
            final IndexedResourceContainer<Future<?>> futures = this.futures;
            
            if (!futures.allocate(index)) {
                return;
            }
            Future<?> f = scheduler.submit(() -> {
                futures.deleteResource(index);
                emit(index + 1);
            });
            futures.setResource(index, f);
        }
        
        @Override
        public void onComplete() {
            long index = producerIndex++;
            final IndexedResourceContainer<Future<?>> futures = this.futures;
            if (!futures.allocate(index)) {
                return;
            }
            done = true;
            Future<?> f = scheduler.schedule(() -> {
                futures.deleteResource(index);
                emit(index + 1);
            }, delay, unit);
            futures.setResource(index, f);
        }
        
        
        void emit(long index) {
            for (;;) {
                long ci = targetIndex;
                long u = Math.max(ci, index);
                if (u == ci || TARGET_INDEX.compareAndSet(this, ci, u)) {
                    break;
                }
            }
            final Queue<T> q = queue;
            final Subscriber<? super T> a = actual;

            if (getAndIncrement() == 0) {
                long ci = consumerIndex;
                do {
                    boolean d = done;
                    if (checkTerminated(d, q.isEmpty(), a)) {
                        return;
                    }
                    long ti = targetIndex;
                    while (ci != ti) {
                        d = done;
                        T v = q.poll();
                        boolean empty = v == null;
                        if (checkTerminated(d, empty, a)) {
                            return;
                        }
                        if (empty) {
                            cancel();
                            a.onError(new AssertionError("Queue was empty!"));
                            return;
                        }
                        
                        a.onNext(v);
                        
                        ci++;
                    }
                    consumerIndex = ci;
                } while (decrementAndGet() != 0);
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
            if (cancelled) {
                return true;
            }
            if (d) {
                Throwable e = error;
                if (e != null) {
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    a.onComplete();
                    return true;
                }
            }
            return false;
        }
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
            futures.close();
        }
    }
}
