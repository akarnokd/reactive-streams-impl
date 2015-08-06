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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.res.Resource;
import com.github.akarnokd.rs.impl.subs.*;

/**
 *
 */
public final class SubscribeOn<T> implements Publisher<T> {
    final Supplier<ExecutorService> executorSupplier;
    final Publisher<? extends T> source;
    public SubscribeOn(Publisher<? extends T> source, Supplier<ExecutorService> executor) {
        this.source = source;
        this.executorSupplier = executor;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        SubscriptionArbiter sa = new SubscriptionArbiter();
        SingleResourceSubscription srs = new SingleResourceSubscription(sa);
        
        s.onSubscribe(srs);
        
        ExecutorService executorService;
        try {
            executorService = executorSupplier.get();
        } catch (Throwable e) {
            s.onError(e);
            return;
        }
        
        if (executorService == null) {
            s.onError(new NullPointerException("The executorSupplier returned null"));
            return;
        }
        
        Future<?> f;
        try {
            f = executorService.submit(() -> {
                source.subscribe(new SubscribeOnSubscriber<>(s, sa));
            });
        } catch (RejectedExecutionException ex) {
            s.onError(ex);
            return;
        }
        
        srs.setResource(Resource.from(f));
    }
    
    static final class SubscribeOnSubscriber<T> extends AtomicBoolean implements Subscriber<T> {
        /** */
        private static final long serialVersionUID = -3875859280644408957L;
        final Subscriber<? super T> actual;
        final SubscriptionArbiter sa;
        public SubscribeOnSubscriber(Subscriber<? super T> actual, SubscriptionArbiter sa) {
            this.actual = actual;
            this.sa = sa;
        }
        @Override
        public void onSubscribe(Subscription s) {
            if (compareAndSet(false, true)) {
                sa.setSubscription(s);
            } else {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
            }
        }
        @Override
        public void onNext(T t) {
            actual.onNext(t);
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
