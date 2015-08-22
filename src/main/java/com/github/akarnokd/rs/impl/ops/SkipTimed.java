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

import java.util.concurrent.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.EmptySubscription;

public final class SkipTimed<T> implements Publisher<T> {
    final Publisher<? extends T> source;
    final long delay;
    final TimeUnit unit;
    final Supplier<? extends ScheduledExecutorService> schedulerSupplier;
    public SkipTimed(Publisher<? extends T> source, long delay, TimeUnit unit,
            Supplier<? extends ScheduledExecutorService> schedulerSupplier) {
        this.source = source;
        this.delay = delay;
        this.unit = unit;
        this.schedulerSupplier = schedulerSupplier;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        ScheduledExecutorService exec;
        try {
            exec = schedulerSupplier.get();
        } catch (Throwable e) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(e);
            return;
        }
        if (exec == null) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(new NullPointerException());
            return;
        }
        
        source.subscribe(new SkipTimedSubscriber<>(s, delay, unit, exec));
    }
    
    static final class SkipTimedSubscriber<T> implements Subscriber<T>, Subscription, Runnable {
        final Subscriber<? super T> actual;
        final long delay;
        final TimeUnit unit;
        final ScheduledExecutorService exec;
        
        Future<?> f;
        Subscription s;
        
        volatile boolean notSkipping;
        boolean notSkippingLocal;
        
        public SkipTimedSubscriber(Subscriber<? super T> actual, long delay, TimeUnit unit,
                ScheduledExecutorService exec) {
            super();
            this.actual = actual;
            this.delay = delay;
            this.unit = unit;
            this.exec = exec;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalArgumentException("Subscription is already set!").printStackTrace();
                return;
            }
            this.s = s;
            this.f = exec.schedule(this, delay, unit);
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(T t) {
            if (notSkippingLocal) {
                actual.onNext(t);
            } else
            if (notSkipping) {
                notSkippingLocal = true;
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            f.cancel(false);
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            f.cancel(false);
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            f.cancel(false);
            s.cancel();
        }
        
        @Override
        public void run() {
            notSkipping = true;
        }
    }
}
