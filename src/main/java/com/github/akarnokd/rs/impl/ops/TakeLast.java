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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.RequestManager;

public final class TakeLast<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final int count;
    
    
    
    public TakeLast(Publisher<? extends T> source, int count) {
        this.source = source;
        this.count = count;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new TakeLastSubscriber<>(s, count));
    }
    
    static final class TakeLastSubscriber<T> extends ArrayDeque<T> implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = 7240042530241604978L;
        final Subscriber<? super T> actual;
        final int count;
        
        Subscription s;
        volatile boolean done;
        volatile boolean cancelled;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<TakeLastSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(TakeLastSubscriber.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<TakeLastSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(TakeLastSubscriber.class, "wip");
        
        public TakeLastSubscriber(Subscriber<? super T> actual, int count) {
            this.actual = actual;
            this.count = count;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!");
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
            s.request(Long.MAX_VALUE);
        }
        
        @Override
        public void onNext(T t) {
            if (count == size()) {
                poll();
            }
            offer(t);
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            done = true;
            drain();
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required but it was " + n);
            }
            RequestManager.add(REQUESTED, this, n);
        }
        
        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) == 0) {
                Subscriber<? super T> a = actual;
                long r = requested;
                do {
                    if (cancelled) {
                        return;
                    }
                    if (done) {
                        boolean unbounded = r == Long.MAX_VALUE;
                        long e = 0L;
                        
                        while (r != 0L) {
                            if (cancelled) {
                                return;
                            }
                            T v = poll();
                            if (v == null) {
                                a.onComplete();
                                return;
                            }
                            a.onNext(v);
                            r--;
                            e++;
                        }
                        if (!unbounded && e != 0L) {
                            r = REQUESTED.addAndGet(this, -e);
                        }
                    }
                } while (WIP.decrementAndGet(this) != 0);
            }
        }
    }
}
