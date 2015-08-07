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

import java.util.function.Predicate;

import org.reactivestreams.*;

/**
 * 
 */
public final class Filter<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final Predicate<? super T> predicate;
    public Filter(Publisher<? extends T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }
    public Publisher<? extends T> source() {
        return source;
    }
    public Predicate<? super T> predicate() {
        return predicate;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new FilterSubscriber<>(s, predicate));
    }
    
    static final class FilterSubscriber<T> implements Subscriber<T> {
        final Predicate<? super T> filter;
        final Subscriber<? super T> actual;
        Subscription subscription;
        public FilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> filter) {
            this.actual = actual;
            this.filter = filter;
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
            boolean b;
            try {
                b = filter.test(t);
            } catch (Throwable e) {
                subscription.cancel();
                actual.onError(e);
                return;
            }
            if (b) {
                actual.onNext(t);
            } else {
                subscription.request(1);
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
