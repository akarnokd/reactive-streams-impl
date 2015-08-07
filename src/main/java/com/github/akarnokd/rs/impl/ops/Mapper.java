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

import java.util.function.Function;

import org.reactivestreams.*;

/**
 * 
 */
public final class Mapper<T, U> implements Publisher<U> {
    final Publisher<? extends T> source;
    final Function<? super T, ? extends U> function;
    public Mapper(Publisher<? extends T> source, Function<? super T, ? extends U> function) {
        this.source = source;
        this.function = function;
    }
    public Publisher<? extends T> source() {
        return source;
    }
    public Function<? super T, ? extends U> function() {
        return function;
    }
    @Override
    public void subscribe(Subscriber<? super U> s) {
        source.subscribe(new MapperSubscriber<>(s, function));
    }
    static final class MapperSubscriber<T, U> implements Subscriber<T> {
        final Subscriber<? super U> actual;
        final Function<? super T, ? extends U> function;
        Subscription subscription;
        public MapperSubscriber(Subscriber<? super U> actual, Function<? super T, ? extends U> function) {
            this.actual = actual;
            this.function = function;
        }
        @Override
        public void onSubscribe(Subscription s) {
            if (subscription != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
                return;
            }
            subscription = s;
            actual.onSubscribe(s);
        }
        @Override
        public void onNext(T t) {
            U u;
            try {
                u = function.apply(t);
            } catch (Throwable e) {
                subscription.cancel();
                actual.onError(e);
                return;
            }
            actual.onNext(u);
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
