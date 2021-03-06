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
    public void subscribe(Subscriber<? super T> child) {
        SerializedSubscriber<T> serial = new SerializedSubscriber<>(child);
        
        FixedResourceContainer<Subscription> frc = new FixedResourceContainer<>(2, Subscription::cancel);
        
        TakeUntilSubscriber<T> tus = new TakeUntilSubscriber<>(serial, frc); 
        
        other.subscribe(new Subscriber<U>() {
            @Override
            public void onSubscribe(Subscription s) {
                frc.setResource(0, s);
                s.request(Long.MAX_VALUE);
            }
            @Override
            public void onNext(U t) {
                frc.close();
                serial.onComplete();
            }
            @Override
            public void onError(Throwable t) {
                frc.close();
                serial.onError(t);
            }
            @Override
            public void onComplete() {
                frc.close();
                serial.onComplete();
            }
        });
        
        source.subscribe(tus);
    }
    
    static final class TakeUntilSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;
        final FixedResourceContainer<Subscription> frc;
        
        Subscription s;
        
        public TakeUntilSubscriber(Subscriber<? super T> actual, FixedResourceContainer<Subscription> frc) {
            this.actual = actual;
            this.frc = frc;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
                return;
            }
            this.s = s;
            frc.setResource(0, s);
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            frc.close();
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            frc.close();
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            frc.close();
        }
    }
}
