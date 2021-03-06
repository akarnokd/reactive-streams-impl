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

import org.reactivestreams.*;

/**
 * 
 */
public final class Skip<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final long n;
    public Skip(Publisher<? extends T> source, long n) {
        this.source = source;
        this.n = n;
    }

    public Publisher<? extends T> source() {
        return source;
    }
    
    public long n() {
        return n;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new SkipSubscriber<>(s, n));
    }
    
    static final class SkipSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        long remaining;
        public SkipSubscriber(Subscriber<? super T> actual, long n) {
            this.actual = actual;
            this.remaining = n;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            long n = remaining;
            actual.onSubscribe(s);
            s.request(n);
        }
        
        @Override
        public void onNext(T t) {
            if (remaining != 0L) {
                remaining--;
            } else {
                actual.onNext(t);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
    }
}
