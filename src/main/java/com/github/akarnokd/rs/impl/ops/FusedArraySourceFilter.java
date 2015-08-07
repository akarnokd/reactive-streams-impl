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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.RequestManager;

/**
 * 
 */
public final class FusedArraySourceFilter<T> implements Publisher<T> {
    final T[] array;
    final Predicate<? super T> predicate;
    public FusedArraySourceFilter(T[] array, Predicate<? super T> predicate) {
        this.array = array;
        this.predicate = predicate;
    }
    public T[] array() {
        return array;
    }
    public Predicate<? super T> predicate() {
        return predicate;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new FusedArraySourceFilterSubscription<>(array, predicate, s));
    }
    static final class FusedArraySourceFilterSubscription<T> extends AtomicLong implements Subscription {
        /** */
        private static final long serialVersionUID = -225561973532207332L;
        
        final T[] array;
        final Subscriber<? super T> subscriber;
        Predicate<? super T> predicate;
        
        int index;
        volatile boolean cancelled;
        
        public FusedArraySourceFilterSubscription(T[] array, Predicate<? super T> predicate, Subscriber<? super T> subscriber) {
            this.array = array;
            this.subscriber = subscriber;
            this.predicate = predicate;
        }
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required").printStackTrace();
                return;
            }
            if (RequestManager.add(this, n) == 0L) {
                long r = n;
                for (;;) {
                    int i = index;
                    T[] a = array;
                    int len = a.length;
                    Subscriber<? super T> s = subscriber;
                    if (i + r >= len) {
                        if (cancelled) {
                            return;
                        }
                        for (int j = i; j < len; j++) {
                            T v = a[j];
                            boolean b;
                            try {
                                b = predicate.test(v);
                            } catch (Throwable ex) {
                                s.onError(ex);
                                return;
                            }
                            if (b) {
                                s.onNext(v);
                            }
                            if (cancelled) {
                                return;
                            }
                        }
                        s.onComplete();
                        return;
                    }
                    long e = 0;
                    if (cancelled) {
                        return;
                    }
                    while (r != 0 && i < len) {
                        T v = a[i];
                        boolean b;
                        try {
                            b = predicate.test(v);
                        } catch (Throwable ex) {
                            s.onError(ex);
                            return;
                        }
                        if (b) {
                            s.onNext(v);
                            r--;
                            e++;
                        }
                        if (cancelled) {
                            return;
                        }
                        if (++i == len) {
                            s.onComplete();
                            return;
                        }
                    }
                    index = i;
                    r = addAndGet(-e);
                    if (r == 0L) {
                        return;
                    }
                }
            }
        }
        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
