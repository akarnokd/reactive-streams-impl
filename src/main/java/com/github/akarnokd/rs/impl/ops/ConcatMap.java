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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.queue.SpscArrayQueue;
import com.github.akarnokd.rs.impl.subs.SubscriptionArbiter;

/**
 * 
 */
public final class ConcatMap<T, U> implements Publisher<U> {
    final Publisher<? extends T> source;
    final Function<? super T, ? extends Publisher<? extends U>> mapper;
    public ConcatMap(Publisher<? extends T> source, Function<? super T, ? extends Publisher<? extends U>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }
    @Override
    public void subscribe(Subscriber<? super U> s) {
        SerializedSubscriber<U> ssub = new SerializedSubscriber<>(s);
        SubscriptionArbiter sa = new SubscriptionArbiter();
        ssub.onSubscribe(sa);
        source.subscribe(new SourceSubscriber<>(ssub, sa, mapper));
    }
    
    static final class SourceSubscriber<T, U> extends AtomicInteger implements Subscriber<T> {
        /** */
        private static final long serialVersionUID = 8828587559905699186L;
        final Subscriber<? super U> actual;
        final SubscriptionArbiter sa;
        final Function<? super T, ? extends Publisher<? extends U>> mapper;
        Subscription s;
        final Subscriber<U> inner;
        final Queue<T> queue;
        
        volatile boolean done;
        
        public SourceSubscriber(Subscriber<? super U> actual, SubscriptionArbiter sa,
                Function<? super T, ? extends Publisher<? extends U>> mapper) {
            this.actual = actual;
            this.sa = sa;
            this.mapper = mapper;
            this.inner = new InnerSubscriber<>(actual, sa, this);
            this.queue = new SpscArrayQueue<>(2);
        }
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();;
                return;
            }
            this.s = s;
            s.request(2);
        }
        @Override
        public void onNext(T t) {
            if (!queue.offer(t)) {
                cancel();
                actual.onError(new IllegalStateException("More values received than requested!"));
                return;
            }
            if (getAndIncrement() == 0) {
                drain();
            }
        }
        @Override
        public void onError(Throwable t) {
            cancel();
            actual.onError(t);
        }
        @Override
        public void onComplete() {
            done = true;
            if (getAndIncrement() == 0) {
                drain();
            }
        }
        
        void innerComplete() {
            if (decrementAndGet() != 0) {
                drain();
            } else
            if (!done) {
                s.request(1);
            }
        }
        
        void cancel() {
            sa.cancel();
            s.cancel();
        }
        
        void drain() {
            boolean d = done;
            T o = queue.poll();
            
            if (o == null) {
                if (d) {
                    actual.onComplete();
                    return;
                }
                new IllegalStateException("Queue is empty?!").printStackTrace();
                return;
            }
            Publisher<? extends U> p;
            try {
                p = mapper.apply(o);
            } catch (Throwable e) {
                cancel();
                actual.onError(e);
                return;
            }
            p.subscribe(inner);
        }
    }
    
    static final class InnerSubscriber<U> implements Subscriber<U> {
        final Subscriber<? super U> actual;
        final SubscriptionArbiter sa;
        final SourceSubscriber<?, ?> parent;
        public InnerSubscriber(Subscriber<? super U> actual, 
                SubscriptionArbiter sa, SourceSubscriber<?, ?> parent) {
            this.actual = actual;
            this.sa = sa;
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            sa.setSubscription(s);
        }
        
        @Override
        public void onNext(U t) {
            actual.onNext(t);
            sa.produced(1L);
        }
        @Override
        public void onError(Throwable t) {
            parent.cancel();
            actual.onError(t);
        }
        @Override
        public void onComplete() {
            parent.innerComplete();
        }
    }
}
