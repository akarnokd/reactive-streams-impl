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

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.EmptySubscription;

/**
 * 
 */
public final class Take<T> implements Publisher<T> {
    final long limit;
    final Publisher<? extends T> source;
    public Take(Publisher<? extends T> source, long limit) {
        this.source = source;
        this.limit = limit;
    }
    public long limit() {
        return limit;
    }
    public Publisher<? extends T> source() {
        return source;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (limit == 0L) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onComplete();
            return;
        }
        source.subscribe(new TakeSubscriber<>(s, limit));
    }
    static final class TakeSubscriber<T> extends AtomicLong implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = -5636543848937116287L;
        boolean done;
        Subscription subscription;
        final Subscriber<? super T> actual;
        final long limit;
        long remaining;
        public TakeSubscriber(Subscriber<? super T> actual, long limit) {
            this.actual = actual;
            this.limit = limit;
            this.remaining = limit;
        }
        @Override
        public void onSubscribe(Subscription s) {
            Subscription s0 = subscription;
            if (s0 != null) {
                s.cancel();
                new IllegalStateException("Subscription already set").printStackTrace();;
            } else {
                subscription = s;
                actual.onSubscribe(this);
            }
        }
        @Override
        public void onNext(T t) {
            if (!done) {
                actual.onNext(t);
                if (--remaining == 0L) {
                    onComplete();
                }
            }
        }
        @Override
        public void onError(Throwable t) {
            if (!done) {
                done = true;
                subscription.cancel();
                actual.onError(t);
            }
        }
        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                subscription.cancel();
                actual.onComplete();
            }
        }
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required").printStackTrace();
                return;
            }
            for (;;) {
                long r = get();
                if (r >= limit) {
                    return;
                }
                long u = r + n;
                if (u < 0) {
                    u = Long.MAX_VALUE;
                }
                if (compareAndSet(r, u)) {
                    if (u >= limit) {
                        subscription.request(Long.MAX_VALUE);
                    } else {
                        subscription.request(n);
                    }
                    return;
                }
            }
        }
        @Override
        public void cancel() {
            subscription.cancel();
        }
    }
}
