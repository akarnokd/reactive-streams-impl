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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.res.Resource;

/**
 * 
 */
public final class CloseableCallbackSubscriber<T> implements Subscriber<T>, Resource {
    final Consumer<? super Subscription> onSubscribeCallback;
    final Consumer<? super T> onNextCallback;
    final Consumer<? super Throwable> onErrorCallback;
    final Runnable onCompleteCallback;
    
    volatile Subscription s;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<CloseableCallbackSubscriber, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(CloseableCallbackSubscriber.class, Subscription.class, "s");
    
    static final ClosedSubscription CLOSED = new ClosedSubscription();
    
    public CloseableCallbackSubscriber(Consumer<? super Subscription> onSubscribeCallback, Consumer<? super T> onNextCallback,
            Consumer<? super Throwable> onErrorCallback, Runnable onCompleteCallback) {
        this.onSubscribeCallback = onSubscribeCallback;
        this.onNextCallback = onNextCallback;
        this.onErrorCallback = onErrorCallback;
        this.onCompleteCallback = onCompleteCallback;
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        for (;;) {
            Subscription curr = s;
            if  (curr == CLOSED) {
                s.cancel();
                return;
            }
            if (curr != null) {
                s.cancel();
                new IllegalArgumentException("Subscription already set!").printStackTrace();
                return;
            }
            if (S.compareAndSet(this, null, s)) {
                break;
            }
        }
        onSubscribeCallback.accept(s);
    }
    
    @Override
    public void onNext(T t) {
        onNextCallback.accept(t);
    }
    
    @Override
    public void onError(Throwable t) {
        onErrorCallback.accept(t);
    }
    
    @Override
    public void onComplete() {
        onCompleteCallback.run();
    }

    @Override
    public void close() {
        Subscription curr = s;
        if (curr != CLOSED) {
            curr = S.getAndSet(this, CLOSED);
            if (curr != CLOSED && curr != null) {
                curr.cancel();
            }
        }
    }
    
    static final class ClosedSubscription implements Subscription {
        @Override
        public void request(long n) {
            
        }
        
        @Override
        public void cancel() {
            
        }
    }
}
