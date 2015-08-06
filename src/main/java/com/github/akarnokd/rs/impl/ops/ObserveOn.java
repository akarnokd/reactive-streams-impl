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
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.queue.SpscArrayQueue;
import com.github.akarnokd.rs.impl.subs.*;
import com.github.akarnokd.rs.impl.util.Util;

/**
 *
 */
public final class ObserveOn<T> implements Publisher<T> {
    final Supplier<ExecutorService> executorSupplier;
    final Publisher<? extends T> source;
    final int bufferSize;
    final boolean delayError;
    public ObserveOn(Publisher<? extends T> source, 
            Supplier<ExecutorService> executorSupplier, 
            int bufferSize, boolean delayError) {
        this.source = source;
        this.executorSupplier = executorSupplier;
        this.bufferSize = bufferSize;
        this.delayError = delayError;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
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
        ObserveOnSubscriber<T> oos = new ObserveOnSubscriber<>(s, bufferSize, delayError, executorService);
        s.onSubscribe(oos);
        source.subscribe(oos);
    }
    
    static final class ObserveOnSubscriber<T> extends AtomicLong implements Subscriber<T>, Subscription, Runnable {
        /** */
        private static final long serialVersionUID = 1802246011184400495L;
        final Subscriber<? super T> actual;
        final Queue<T> queue;
        final boolean delayError;
        final ExecutorService executor;
        private final int bufferSize;
        
        volatile boolean done;
        Throwable error;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ObserveOnSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ObserveOnSubscriber.class, "wip");
        
        volatile Future<?> future;
        
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ObserveOnSubscriber, Future> FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(ObserveOnSubscriber.class, Future.class, "future");

        volatile boolean cancelled;
        
        static final Future<?> EMPTY_FUTURE = new EmptyFuture();
        
        static final Future<?> CANCELLED_FUTURE = new EmptyFuture();
        
        static final Subscription CANCELLED_SUBSCRIPTION = new CancelledSubscription();
        
        volatile Subscription subscription;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ObserveOnSubscriber, Subscription> SUBSCRIPTION =
                AtomicReferenceFieldUpdater.newUpdater(ObserveOnSubscriber.class, Subscription.class, "subscription");
        
        public ObserveOnSubscriber(Subscriber<? super T> actual, 
                int bufferSize, boolean delayError,
                ExecutorService executor) {
            this.actual = actual;
            this.bufferSize = bufferSize;
            this.queue = new SpscArrayQueue<>(bufferSize);
            this.delayError = delayError;
            this.executor = executor;
            FUTURE.lazySet(this, EMPTY_FUTURE);
        }
        @Override
        public void onSubscribe(Subscription s) {
            for (;;) {
                Subscription curr = subscription;
                if (curr == CANCELLED_SUBSCRIPTION) {
                    s.cancel();
                    return;
                } else
                if (curr != null) {
                    s.cancel();
                    new IllegalStateException("Subscription already set!").printStackTrace();
                    return;
                }
                if (SUBSCRIPTION.compareAndSet(this, null, s)) {
                    s.request(bufferSize);
                    return;
                }
            } 
        }
        @Override
        public void onNext(T t) {
            if (!queue.offer(t)) {
                onError(new IllegalStateException("OnNext received beyond requested"));
            } else {
                schedule();
            }
        }
        @Override
        public void onError(Throwable t) {
            if (!done) {
                error = t;
                done = true;
                schedule();
            }
        }
        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                schedule();
            }
        }
        @Override
        public void request(long n) {
            RequestManager.add(this, n);
            schedule();
        }
        
        void schedule() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }

            Future<?> curr = future;
            Future<?> f;
            try {
                f = executor.submit(this);
            } catch (RejectedExecutionException ex) {
                cancel();
                actual.onError(ex);
                return;
            }
            
            if (!FUTURE.compareAndSet(this, curr, f)) {
                if (future == CANCELLED_FUTURE) {
                    f.cancel(true);
                }
            }
        }
        
        @Override
        public void run() {
            try {
                emissionLoop();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        
        void emissionLoop() {
            long r = get();
            final long r0 = r;
            Future<?> f = null;
            final Queue<T> q = queue;
            final Subscriber<? super T> s = actual;
            final boolean de = delayError;
            Subscription sub = subscription;
            
            do {
                boolean d = done;
                boolean empty = q.isEmpty();
                
                if (checkFinish(d, empty, de, s)) {
                    return;
                }

                long e = 0L;
                while (r != 0L) {
                    d = done;
                    T value = q.poll();
                    empty = value == null;

                    if (checkFinish(d, empty, de, s)) {
                        return;
                    }
                    if (empty) {
                        break;
                    }
                    
                    s.onNext(value);
                    
                    if (cancelled) {
                        return;
                    }
                    
                    e++;
                    r--;
                }
                if (e != 0L) {
                    if (sub == null) {
                        sub = subscription;
                        if (sub == null) {
                            new IllegalStateException("Emission without subscription").printStackTrace();
                        }
                    }
                    sub.request(e);
                    if (r0 != Long.MAX_VALUE) {
                        r = addAndGet(-e);
                    } else {
                        r = r0;
                    }
                }
                if (wip == 1) {
                    f = future;
                }
            } while (WIP.decrementAndGet(this) != 0);
            
            FUTURE.compareAndSet(this, f, EMPTY_FUTURE);
        }
        
        boolean checkFinish(boolean d, boolean empty, boolean de, Subscriber<? super T> s) {
            if (cancelled) {
                return true;
            }
            if (d) {
                if (de) {
                    if (empty) {
                        Throwable err = error;
                        cancelSubscription();
                        FUTURE.lazySet(this, CANCELLED_FUTURE);
                        if (err == null) {
                            s.onComplete();
                        } else {
                            error = null;
                            s.onError(err);
                        }
                        return true;
                    }
                } else {
                    Throwable err = error;
                    if (err == null) {
                        if (!empty) {
                            return false;
                        }
                    }
                    cancelSubscription();
                    FUTURE.lazySet(this, CANCELLED_FUTURE);
                    if (err == null) {
                        s.onComplete();
                    } else {
                        error = null;
                        s.onError(err);
                    }
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelSubscription();
                Util.atomicTerminate(FUTURE, this, CANCELLED_FUTURE, v -> v.cancel(true));
            }
        }
        void cancelSubscription() {
            Util.atomicTerminate(SUBSCRIPTION, this, CANCELLED_SUBSCRIPTION, v -> {
                if (v != null) {
                    v.cancel();
                }
            });
        }
    }
    
    static final class EmptyFuture implements Future<Object> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
        
    }
    static final class CancelledSubscription implements Subscription {
        @Override
        public void request(long n) {
            
        }
        @Override
        public void cancel() {
            
        }
    }
}
