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

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.*;

/**
 *
 */
public final class SumLong implements Publisher<Long> {
    final Publisher<? extends Number> source;
    public SumLong(Publisher<? extends Number> source) {
        this.source = source;
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        source.subscribe(new SumLongSubscriber(s));
    }
    
    static final class SumLongSubscriber extends AtomicBoolean implements Subscriber<Number>, Subscription {
        /** */
        private static final long serialVersionUID = -6186305055696101343L;
        
        final Subscriber<? super Long> actual;
        long sum;
        boolean hasValue;
        
        Subscription s;
        
        public SumLongSubscriber(Subscriber<? super Long> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscritpion already set!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(Number t) {
            if (!hasValue) {
                hasValue = true;
            }
            
            sum += t.longValue();
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (hasValue) {
                actual.onNext(sum);
            }
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > required but it was " + n);
                return;
            }
            if (compareAndSet(false, true)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
    }
}
