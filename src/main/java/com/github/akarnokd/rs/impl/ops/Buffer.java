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

import java.util.Collection;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.RequestManager;

public final class Buffer<T, U extends Collection<T>> implements Publisher<U> {
    final Publisher<? extends T> source;
    final Supplier<U> bufferSupplier;
    final int bufferSize;
    public Buffer(Publisher<? extends T> source, Supplier<U> bufferSupplier, int bufferSize) {
        this.source = source;
        this.bufferSupplier = bufferSupplier;
        this.bufferSize = bufferSize;
    }
    
    @Override
    public void subscribe(Subscriber<? super U> s) {
        source.subscribe(new BufferSubscriber<>(bufferSupplier, bufferSize, s));
    }
    
    static final class BufferSubscriber<T, U extends Collection<T>> implements Subscriber<T>, Subscription {
        final Supplier<U> bufferSupplier;
        final int bufferSize;
        final Subscriber<? super U> actual;
        
        Subscription s;
        U buffer;
        int count;
        
        public BufferSubscriber(Supplier<U> bufferSupplier, int bufferSize, Subscriber<? super U> actual) {
            super();
            this.bufferSupplier = bufferSupplier;
            this.bufferSize = bufferSize;
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(T t) {
            U b = buffer;
            if (b == null) {
                try {
                    b = bufferSupplier.get();
                } catch (Throwable e) {
                    s.cancel();
                    actual.onError(e);
                    return;
                }
                buffer = b;
            }
            b.add(t);
            if (++count >= bufferSize) {
                count = 0;
                buffer = null;
                actual.onNext(b);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            U b = buffer;
            if (b != null) {
                buffer = null;
                actual.onNext(b);
            }
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (n <= 0L) {
                new IllegalArgumentException("n > 0 required but it was " + n).printStackTrace();
                return;
            }
            long u = RequestManager.multiplyAndCap(n, bufferSize);
            s.request(u);
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
    }
}
