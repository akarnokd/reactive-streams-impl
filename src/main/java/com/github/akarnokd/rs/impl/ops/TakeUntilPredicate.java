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
public final class TakeUntilPredicate<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final Predicate<? super T> predicate;
    public TakeUntilPredicate(Publisher<? extends T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new InnerSubscriber<>(s, predicate));
    }
    
    static final class InnerSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        final Predicate<? super T> predicate;
        Subscription s;
        boolean done;
        public InnerSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(s);
        }
        
        @Override
        public void onNext(T t) {
            if (!done) {
                actual.onNext(t);
                boolean b;
                try {
                    b = predicate.test(t);
                } catch (Throwable e) {
                    done = true;
                    s.cancel();
                    actual.onError(e);
                    return;
                }
                if (b) {
                    done = true;
                    s.cancel();
                    actual.onComplete();
                }
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (!done) {
                done = true;
                actual.onError(t);
            }
        }
        
        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                actual.onComplete();
            }
        }
    }
}
