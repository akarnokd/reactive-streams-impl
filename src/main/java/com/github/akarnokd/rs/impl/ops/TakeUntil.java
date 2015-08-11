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

import com.github.akarnokd.rs.impl.res.*;

/**
 * 
 */
public final class TakeUntil<T, U> implements Publisher<T> {
    final Publisher<? extends T> source;
    final Publisher<? extends U> other;
    public TakeUntil(Publisher<? extends T> source, Publisher<? extends U> other) {
        this.source = source;
        this.other = other;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);
        
        FixedResourceList frl = new FixedResourceList(2);
        
        other.subscribe(new Subscriber<U>() {
            @Override
            public void onSubscribe(Subscription s) {
                frl.set(0, s::cancel);
                s.request(Long.MAX_VALUE);
            }
            @Override
            public void onNext(U t) {
                frl.close();
                serial.onComplete();
            }
            @Override
            public void onError(Throwable t) {
                frl.close();
                serial.onError(t);
            }
            @Override
            public void onComplete() {
                frl.close();
                serial.onComplete();
            }
        });
        
        source.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(Subscription s) {
                frl.set(1, s::cancel);
                serial.onSubscribe(s);
            }
            @Override
            public void onNext(T t) {
                serial.onNext(t);
            }
            @Override
            public void onError(Throwable t) {
                frl.close();
                serial.onError(t);
            }
            @Override
            public void onComplete() {
                frl.close();
                serial.onComplete();
            }
        });
    }
}
