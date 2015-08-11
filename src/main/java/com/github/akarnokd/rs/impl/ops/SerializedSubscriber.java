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
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.queue.MpscLinkedQueue;

/**
 * 
 */
public final class SerializedSubscriber<T> extends AtomicInteger implements Subscriber<T>, Subscription {
    /** */
    private static final long serialVersionUID = -8745324087297179297L;
    final Subscriber<? super T> actual;
    final MpscLinkedQueue<T> queue;
    
    private volatile Object terminal;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<SerializedSubscriber, Object> TERMINAL =
            AtomicReferenceFieldUpdater.newUpdater(SerializedSubscriber.class, Object.class, "terminal");
    
    private static final Object DONE = new Object();
    
    private volatile boolean cancelled;
    
    private Subscription subscription;
    
    public SerializedSubscriber(Subscriber<? super T> actual) {
        this.actual = actual;
        this.queue = new MpscLinkedQueue<>();
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        if (subscription != null) {
            s.cancel();
            new IllegalStateException("Subscription already set!").printStackTrace();
            return;
        }
        this.subscription = s;
        actual.onSubscribe(this);
    }
    
    @Override
    public void request(long n) {
        subscription.request(n);
    }
    @Override
    public void cancel() {
        cancelled = true;
        subscription.cancel();
    }
    
    @Override
    public void onNext(T t) {
        if (terminal != null) {
            return;
        }
        if (get() == 0 && compareAndSet(0, 1)) {
            actual.onNext(t);
            if (decrementAndGet() == 0) {
                return;
            }
        } else {
            if (terminal != null) {
                return;
            }
            queue.offer(t);
            if (getAndIncrement() != 0) {
                return;
            }
        }
        drain();
    }
    
    @Override
    public void onError(Throwable t) {
        if (get() == 0 && compareAndSet(0, 1)) {
            terminal = DONE;
            actual.onError(t);
            return;
        } else {
            if (!TERMINAL.compareAndSet(this, null, t)) {
                t.printStackTrace();
                return;
            }
            if (getAndIncrement() != 0) {
                return;
            }
        }
        drain();
    }
    
    @Override
    public void onComplete() {
        if (get() == 0 && compareAndSet(0, 1)) {
            terminal = DONE;
            actual.onComplete();
            return;
        } else {
            if (!TERMINAL.compareAndSet(this, null, DONE)) {
                return;
            }
            if (getAndIncrement() != 0) {
                return;
            }
        }
        drain();
    }
    
    void drain() {
        final Subscriber<? super T> actual = this.actual;
        final Queue<T> queue = this.queue;
        do {
            if (cancelled) {
                terminal = DONE;
                queue.clear();
                return;
            }
            Object d = terminal;
            if (d != null) {
                if (d == DONE) {
                    actual.onComplete();
                } else {
                    terminal = DONE;
                    actual.onError((Throwable)d);
                }
                return;
            }
            for (;;) {
                T o = queue.poll();
                if (o != null && !cancelled) {
                    actual.onNext(o);
                } else {
                    break;
                }
            }
            if (cancelled) {
                terminal = DONE;
                queue.clear();
                return;
            }
            
        } while (decrementAndGet() != 0);
    }
}
