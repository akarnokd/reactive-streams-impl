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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.res.*;
import com.github.akarnokd.rs.impl.subs.*;

/**
 *
 */
public final class ManySelect<T, R> implements Publisher<R> {
    final Publisher<? extends T> source;
    final Function<? super Publisher<? extends T>, R> mapper;
    final Supplier<? extends ExecutorService> schedulerSupplier;
    /**
     * @param source
     * @param mapper
     * @param schedulerSupplier
     */
    public ManySelect(Publisher<? extends T> source, Function<? super Publisher<? extends T>, R> mapper,
            Supplier<? extends ExecutorService> schedulerSupplier) {
        this.source = source;
        this.mapper = mapper;
        this.schedulerSupplier = schedulerSupplier;
    }
    
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        ExecutorService exec;
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
        
        SerializedSubscriber<R> serial = new SerializedSubscriber<>(s);
        
        source.subscribe(new ManySelectSubscriber<>(serial, mapper, exec));
    }
    
    static final class ManySelectSubscriber<T, R> extends AtomicInteger implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = 7793795070766455716L;
        final Subscriber<? super R> actual;
        final Function<? super Publisher<? extends T>, R> mapper;
        final ExecutorService exec;
        // FIXME use a more generic container
        final IndexedResourceContainer<Future<?>> tasks;
        // FIXME use linked array list
        volatile List<Object> queue;

        volatile boolean done;
        Throwable error;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<ManySelectSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(ManySelectSubscriber.class, "requested");
        
        Subscription s;
        
        public ManySelectSubscriber(Subscriber<? super R> actual, 
                Function<? super Publisher<? extends T>, R> mapper,
                ExecutorService exec) {
            this.actual = actual;
            this.mapper = mapper;
            this.exec = exec;
            this.tasks = new IndexedResourceContainer<>(256, f -> f.cancel(true));
            this.queue = new ArrayList<>();
            lazySet(1);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscritpion already set!").printStackTrace();
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
            s.request(Long.MAX_VALUE);
        }
        
        @Override
        public void onNext(T t) {
            List<Object> q = queue;
            if (q == null) {
                return;
            }
            
            getAndIncrement();
            
            int s = q.size();
            int unique = s >> 1;

            if (!tasks.allocate(unique)) {
                return;
            }

            ManySelectTaskPublisher<T, R> task = new ManySelectTaskPublisher<>(this, mapper, unique);
            
            q.add(t);
            q.add(task);
            
            Future<?> f = exec.submit(task);
            tasks.setResource(unique, f);
            
            s += 2;
            
            for (int i = 1; i < s; i += 2) {
                @SuppressWarnings("unchecked")
                ManySelectTaskPublisher<T, R> o = (ManySelectTaskPublisher<T, R>)q.get(i);
                o.maxIndex = s;
                o.drain();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            List<Object> q = queue;
            cancel();
            if (q != null) {
                int s = q.size();
                for (int i = 1; i < s; i += 2) {
                    @SuppressWarnings("unchecked")
                    ManySelectTaskPublisher<T, R> o = (ManySelectTaskPublisher<T, R>)q.get(i);
                    
                    o.drain();
                }
            }
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            done = true;
            List<Object> q = queue;
            if (q != null) {
                int s = q.size();
                for (int i = 1; i < s; i += 2) {
                    @SuppressWarnings("unchecked")
                    ManySelectTaskPublisher<T, R> o = (ManySelectTaskPublisher<T, R>)q.get(i);
                    o.drain();
                }
            }
            
            if (decrementAndGet() == 0) {
                actual.onComplete();
            }
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required but it was " + n);
                return;
            }
            RequestManager.add(REQUESTED, this, n);
        }
        
        @Override
        public void cancel() {
            queue = null;
            tasks.close();
            s.cancel();
        }
    }
    
    static final class ManySelectTaskPublisher<T, R> extends AtomicInteger implements Publisher<T>, Runnable, Subscription {
        /** */
        private static final long serialVersionUID = -3968273677824996398L;
        final ManySelectSubscriber<T, R> parent;
        final Function<? super Publisher<? extends T>, R> mapper;
        Subscriber<? super T> child;
        final int index;
        
        int currentIndex;
        volatile int maxIndex;
        
        volatile boolean cancelled;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ManySelectTaskPublisher> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ManySelectTaskPublisher.class, "once");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<ManySelectTaskPublisher> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(ManySelectTaskPublisher.class, "requested");
        
        public ManySelectTaskPublisher(ManySelectSubscriber<T, R> parent,
                Function<? super Publisher<? extends T>, R> mapper, 
                int currentIndex) {
            this.parent = parent;
            this.mapper = mapper;
            this.index = currentIndex;
            this.currentIndex = currentIndex << 1;
        }

        @Override
        public void run() {
            final ManySelectSubscriber<T, R> p = parent;
            try {
                final Subscriber<? super R> a = p.actual;
                R v;
                try {
                    v = mapper.apply(this);
                } catch (Throwable e) {
                    p.cancel();
                    a.onError(e);
                    return;
                }
                
                long r = p.requested;
                if (r != 0L) {
                    a.onNext(v);
                    if (r != Long.MAX_VALUE) {
                        ManySelectSubscriber.REQUESTED.decrementAndGet(p);
                    }
                    
                    if (p.decrementAndGet() == 0) {
                        a.onComplete();
                    }
                } else {
                    p.cancel();
                    a.onError(new IllegalStateException("Can't emit value due to lack of requests!"));
                }
            } finally {
                p.tasks.deleteResource(index);
            }
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                this.child = s;
                s.onSubscribe(this);
            } else {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(new IllegalStateException("Only one subscriber allowed"));
            }
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required but it was " + n).printStackTrace();;
                return;
            }
            RequestManager.add(REQUESTED, this, n);
            drain();
        }
        
        @Override
        public void cancel() {
            cancelled = true;
        }
        
        void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                
                final ManySelectSubscriber<T, R> p = parent;
                final List<Object> q = p.queue;
                Subscriber<? super T> child = this.child;
                long r = requested;
                int idx = currentIndex;
                
                for (;;) {
                    boolean d = p.done;
                    int maxIdx = maxIndex;
                    long e = 0L;
                    boolean unbounded = r == Long.MAX_VALUE;

                    if (checkTerminated(d, idx == maxIdx, child, p)) {
                        return;
                    }
                    
                    while (idx < maxIdx && r != 0L) {
                        @SuppressWarnings("unchecked")
                        T t = (T)q.get(idx);
                        child.onNext(t);
                        
                        if (cancelled) {
                            return;
                        }
                        idx += 2;
                        r--;
                        e--;
                    }
                    
                    if (checkTerminated(p.done, idx == maxIndex, child, p)) {
                        return;
                    }
                    
                    if (e != 0L) {
                        if (!unbounded) {
                            r = REQUESTED.addAndGet(this, e);
                        } else {
                            r = Long.MAX_VALUE;
                        }
                    }
                    
                    currentIndex = idx;
                    
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }

                    if (!unbounded) {
                        r = requested;
                    }
                    if (child == null) {
                        child = this.child;
                    }
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> c, ManySelectSubscriber<T, R> p) {
            if (cancelled) {
                return true;
            }
            if (d) {
                Throwable e = p.error;
                if (e != null) {
                    c.onError(e);
                    return true;
                } else
                if (empty) {
                    c.onComplete();
                    return true;
                }
            }
            return false;
        }
    }
}
