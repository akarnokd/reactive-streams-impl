package com.github.akarnokd.rs.impl.ops;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.queue.SpscLinkedArrayQueue;
import com.github.akarnokd.rs.impl.subs.*;
import com.github.akarnokd.rs.impl.util.Util;

public final class BufferTimed<T, U extends Collection<T>> implements Publisher<U> {
    final Publisher<? extends T> source;
    final long period;
    final TimeUnit unit;
    final Supplier<? extends ScheduledExecutorService> schedulerSupplier;
    final Supplier<U> bufferSupplier;
    final int queueSize;
    public BufferTimed(Publisher<? extends T> source, long period, TimeUnit unit,
            Supplier<? extends ScheduledExecutorService> schedulerSupplier, Supplier<U> bufferSupplier, int queueSize) {
        this.source = source;
        this.period = period;
        this.unit = unit;
        this.schedulerSupplier = schedulerSupplier;
        this.bufferSupplier = bufferSupplier;
        this.queueSize = queueSize;
    }
    
    @Override
    public void subscribe(Subscriber<? super U> s) {
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
        
        source.subscribe(new BufferTimedSubscriber<>(s, period, unit, exec, bufferSupplier, queueSize));
    }
    
    static final class BufferTimedSubscriber<T, U extends Collection<? super T>>
    extends AtomicInteger
    implements Subscriber<T>, Subscription, Runnable {
        /** */
        private static final long serialVersionUID = 7111363066649680424L;
        final Subscriber<? super U> actual;
        final long period;
        final TimeUnit unit;
        final ScheduledExecutorService exec;
        final Supplier<U> bufferSupplier;
        final int queueSize;
        
        Subscription s;
        Future<?> f;
        
        U buffer;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<BufferTimedSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(BufferTimedSubscriber.class, "requested");
        
        volatile Queue<Object> queue;
        
        static final Object EMIT_BUFFER = new Object();
        
        volatile boolean done;
        Throwable error;
        
        volatile boolean cancelled;
        
        public BufferTimedSubscriber(Subscriber<? super U> actual, long period, TimeUnit unit,
                ScheduledExecutorService exec, Supplier<U> bufferSupplier, int queueSize) {
            this.actual = actual;
            this.period = period;
            this.unit = unit;
            this.exec = exec;
            this.bufferSupplier = bufferSupplier;
            this.queueSize = queueSize;
        }

        Queue<Object> queue() {
            Queue<Object> q = queue;
            if (q == null) {
                q = new SpscLinkedArrayQueue<>(queueSize);
                queue = q;
            }
            return q;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (this.s != null) {
                s.cancel();
                new IllegalStateException("Subscription already set!").printStackTrace();
                return;
            }
            this.s = s;
            
            try {
                buffer = bufferSupplier.get();
            } catch (Throwable e) {
                s.cancel();
                actual.onSubscribe(EmptySubscription.INSTANCE);
                actual.onError(e);
                return;
            }
            
            f = exec.scheduleAtFixedRate(this, period, period, unit);
            actual.onSubscribe(this);
            s.request(Long.MAX_VALUE);
        }
        
        @Override
        public void onNext(T t) {
            Util.fastpath(this, () -> {
                buffer.add(t);
            }, () -> {
                queue().offer(t);
            }, this::drain);
        }
        
        @Override
        public void onError(Throwable t) {
            Util.fastpathIf(this, () -> {
                cancel();
                actual.onError(t);
                return true;
            }, () -> {
                error = t;
                done = true;
                return false;
            }, this::drain);
        }
        
        @Override
        public void onComplete() {
            Util.fastpathIf(this, () -> {
                cancel();
                emitFinal(actual);
                return true;
            }, () -> {
                done = true;
                return false;
            }, this::drain);
        }
        
        @SuppressWarnings("unchecked")
        void drain() {
            final Subscriber<? super U> a = actual;
            final Queue<Object> q = queue;
            U b = buffer;
            long r = requested;
            do {
                boolean d = done;
                boolean empty = q == null || q.isEmpty();
                if (checkTerminated(d, empty, a)) {
                    return;
                }
                
                if (q != null) {
                    for (;;) {
                        d = done;
                        Object o = q.poll();
                        empty = o == null;
                        if (checkTerminated(d, empty, a)) {
                            return;
                        }
                        if (empty) {
                            break;
                        }
                        if (o == EMIT_BUFFER) {
                            if (r != 0L) {
                                a.onNext(b);
                                if (r != Long.MAX_VALUE) {
                                    r = REQUESTED.decrementAndGet(this);
                                }
                                try {
                                    b = bufferSupplier.get();
                                } catch (Throwable e) {
                                    cancel();
                                    a.onError(e);
                                    return;
                                }
                                buffer = b;
                            } else {
                                cancel();
                                a.onError(new IllegalStateException("Can't emit buffer due to lack of requests."));
                                return;
                            }
                        } else {
                            b.add((T)o);
                        }
                    }
                }
            } while (decrementAndGet() != 0);
        }
        
        void emitFinal(Subscriber<? super U> a) {
            long r = requested;
            if (r != 0) {
                a.onNext(buffer);
                a.onComplete();
            } else {
                a.onError(new IllegalStateException("Can't emit final buffer due to lack of requests."));
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<? super U> a) {
            if (cancelled) {
                return true;
            }
            if (d) {
                Throwable e = error;
                if (e != null) {
                    cancel();
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    emitFinal(a);
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required but it was " + n).printStackTrace();
                return;
            }
            RequestManager.add(REQUESTED, this, n);
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                f.cancel(true);
                s.cancel();
            }
        }
        
        @Override
        public void run() {
            Util.fastpathIf(this, () -> {
                long r = requested;
                if (r != 0L) {
                    actual.onNext(buffer);
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    try {
                        buffer = bufferSupplier.get();
                    } catch (Throwable e) {
                        cancel();
                        actual.onError(e);
                        return true;
                    }
                } else {
                    cancel();
                    actual.onError(new IllegalStateException("Can't emit buffer due to lack of requests."));
                    return true;
                }
                return false;
            }, () -> {
                queue().offer(EMIT_BUFFER);
                return false;
            }, this::drain);
        }
    }
}
