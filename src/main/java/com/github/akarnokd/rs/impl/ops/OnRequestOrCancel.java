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

import java.util.function.LongConsumer;

import org.reactivestreams.*;

/**
 * 
 */
public final class OnRequestOrCancel<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final LongConsumer onRequestCallback;
    final Runnable onCancelCallback;
    public OnRequestOrCancel(Publisher<? extends T> source, LongConsumer onRequestCallback, Runnable onCancelCallback) {
        this.source = source;
        this.onRequestCallback = onRequestCallback;
        this.onCancelCallback = onCancelCallback;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new OnRequestOrCancelSubscriber<>(s, onRequestCallback, onCancelCallback));
    }
    
    static final class OnRequestOrCancelSubscriber<T> implements Subscriber<T>, Subscription {
        final LongConsumer onRequestCallback;
        final Runnable onCancelCallback;
        final Subscriber<? super T> actual;
        Subscription s;
        
        public OnRequestOrCancelSubscriber(Subscriber<? super T> actual, LongConsumer onRequestCallback, Runnable onCancelCallback) {
            this.actual = actual;
            this.onRequestCallback = onRequestCallback;
            this.onCancelCallback = onCancelCallback;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalArgumentException("Subscription already set!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
        }
        
        @Override
        public void request(long n) {
            onRequestCallback.accept(n);
            s.request(n);
        }
        
        @Override
        public void cancel() {
            onCancelCallback.run();
            s.cancel();
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
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
