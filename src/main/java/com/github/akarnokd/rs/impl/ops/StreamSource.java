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
import java.util.stream.Stream;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.*;

/**
 *
 */
public final class StreamSource<T> extends AtomicBoolean implements Publisher<T> {
    /** */
    private static final long serialVersionUID = 9051303031779816842L;
    
    final Stream<? extends T> stream;
    public StreamSource(Stream<? extends T> stream) {
        this.stream = stream;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (compareAndSet(false, true)) {
            Iterator<? extends T> it;
            try {
                it = stream.iterator();
            } catch (Throwable e) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(e);
                return;
            }
            s.onSubscribe(new StreamSourceSubscription<>(stream, it, s));
            return;
        }
        s.onSubscribe(EmptySubscription.INSTANCE);
        s.onError(new IllegalStateException("Contents already consumed"));
    }
    
    static final class StreamSourceSubscription<T> extends AtomicLong implements Subscription {
        /** */
        private static final long serialVersionUID = 8931425802102883003L;
        final Iterator<? extends T> it;
        final Stream<? extends T> stream;
        final Subscriber<? super T> subscriber;
        
        volatile boolean cancelled;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<StreamSourceSubscription> WIP =
                AtomicIntegerFieldUpdater.newUpdater(StreamSourceSubscription.class, "wip");
        
        public StreamSourceSubscription(Stream<? extends T> stream, Iterator<? extends T> it, Subscriber<? super T> subscriber) {
            this.stream = stream;
            this.it = it;
            this.subscriber = subscriber;
        }
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required").printStackTrace();
                return;
            }
            RequestManager.add(this, n);
            drain();
        }
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                drain();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            long r = get();
            long r0 = r;
            do {
                if (cancelled) {
                    stream.close();
                    return;
                }
                long e = 0L;
                
                if (!it.hasNext()) {
                    subscriber.onComplete();
                    return;
                }
                while (r != 0L) {
                    T v = it.next();
                    subscriber.onNext(v);
                    if (cancelled) {
                        stream.close();
                        return;
                    }
                    if (!it.hasNext()) {
                        subscriber.onComplete();
                        return;
                    }
                    r--;
                    e++;
                }
                if (e != 0 && r0 != Long.MAX_VALUE) {
                    r = addAndGet(e);
                }
            } while (WIP.decrementAndGet(this) != 0);
        }
    }
}
