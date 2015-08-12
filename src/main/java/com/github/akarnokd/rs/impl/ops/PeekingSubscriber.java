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
public final class PeekingSubscriber<T> implements Subscriber<T> {
    final Subscriber<? super T> actual;
    final Subscriber<? super T> peeker;
    public PeekingSubscriber(Subscriber<? super T> actual, Subscriber<? super T> peeker) {
        this.actual = actual;
        this.peeker = peeker;
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        peeker.onSubscribe(s);
        actual.onSubscribe(s);
    }
    
    @Override
    public void onNext(T t) {
        peeker.onNext(t);
        actual.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        peeker.onError(t);
        actual.onError(t);
    }
    
    @Override
    public void onComplete() {
        peeker.onComplete();
        actual.onComplete();
    }
}
