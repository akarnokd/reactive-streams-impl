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

import java.util.function.Consumer;

import org.reactivestreams.*;

/**
 * 
 */
public final class CallbackSubscriber<T> implements Subscriber<T> {
    final Consumer<? super Subscription> onSubscribeCallback;
    final Consumer<? super T> onNextCallback;
    final Consumer<? super Throwable> onErrorCallback;
    final Runnable onCompleteCallback;
    public CallbackSubscriber(Consumer<? super Subscription> onSubscribeCallback, Consumer<? super T> onNextCallback,
            Consumer<? super Throwable> onErrorCallback, Runnable onCompleteCallback) {
        this.onSubscribeCallback = onSubscribeCallback;
        this.onNextCallback = onNextCallback;
        this.onErrorCallback = onErrorCallback;
        this.onCompleteCallback = onCompleteCallback;
    }
    
    public Consumer<? super Subscription> onSubscribeCallback() {
        return onSubscribeCallback;
    }

    public Consumer<? super T> onNextCallback() {
        return onNextCallback;
    }

    public Consumer<? super Throwable> onErrorCallback() {
        return onErrorCallback;
    }

    public Runnable onCompleteCallback() {
        return onCompleteCallback;
    }

    @Override
    public void onSubscribe(Subscription s) {
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
    
}
