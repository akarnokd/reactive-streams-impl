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

import java.util.Iterator;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.*;

/**
 *
 */
public final class IterableSource<T> extends AtomicBoolean implements Publisher<T> {
    /** */
    private static final long serialVersionUID = 9051303031779816842L;
    
    final Iterable<? extends T> source;
    public IterableSource(Iterable<? extends T> source) {
        this.source = source;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;
        try {
            it = source.iterator();
        } catch (Throwable e) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(e);
            return;
        }
        s.onSubscribe(new IteratorSourceSubscription<>(it, s));
    }
    
    static final class IteratorSourceSubscription<T> extends AtomicLong implements Subscription {
        /** */
        private static final long serialVersionUID = 8931425802102883003L;
        final Iterator<? extends T> it;
        final Subscriber<? super T> subscriber;
        
        volatile boolean cancelled;
        
        public IteratorSourceSubscription(Iterator<? extends T> it, Subscriber<? super T> subscriber) {
            this.it = it;
            this.subscriber = subscriber;
        }
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required").printStackTrace();
                return;
            }
            if (RequestManager.add(this, n) != 0L) {
                return;
            }
            long r = n;
            long r0 = n;
            for (;;) {
                if (cancelled) {
                    return;
                }
                if (!it.hasNext()) {
                    subscriber.onComplete();
                    return;
                }
                long e = 0;
                while (r != 0L) {
                    T v = it.next();
                    subscriber.onNext(v);
                    if (cancelled) {
                        return;
                    }
                    if (!it.hasNext()) {
                        subscriber.onComplete();
                        return;
                    }
                    r--;
                    e++;
                }
                if (e != 0L && r0 != Long.MAX_VALUE) {
                    r = addAndGet(-e);
                }
                if (r == 0L) {
                    break;
                }
            }
        }
        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
