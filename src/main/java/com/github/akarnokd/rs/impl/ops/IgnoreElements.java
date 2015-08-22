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

public final class IgnoreElements<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    
    public IgnoreElements(Publisher<? extends T> source) {
        this.source = source;
    }
    @Override
    public void subscribe(Subscriber<? super T> child) {
        source.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(Subscription s) {
                child.onSubscribe(s);
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(T t) {
                // deliberately ignoring values
            }
            
            @Override
            public void onError(Throwable t) {
                child.onError(t);
            }
            
            @Override
            public void onComplete() {
                child.onComplete();
            }
        });
    }
}
