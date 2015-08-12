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
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.res.Resource;
import com.github.akarnokd.rs.impl.subs.*;

/**
 * 
 */
public final class PeriodicTimerSource implements Publisher<Long> {
    final long initialDelay;
    final long period;
    final TimeUnit unit;
    final Supplier<? extends ScheduledExecutorService> schedulerProvider;
    public PeriodicTimerSource(long initialDelay, long period, TimeUnit unit, Supplier<? extends ScheduledExecutorService> schedulerProvider) {
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = unit;
        this.schedulerProvider = schedulerProvider;
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        // TODO Auto-generated method stub
        ScheduledExecutorService exec;
        try {
            exec = schedulerProvider.get();
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
        
        PeriodicSubscription ps = new PeriodicSubscription(s);
        s.onSubscribe(ps);
        
        Future<?> f;
        try {
            f = exec.scheduleAtFixedRate(ps, initialDelay, period, unit);
        } catch (RejectedExecutionException e) {
            s.onError(e);
            return;
        }
        ps.setResource(Resource.from(f));
    }
    
    static final class PeriodicSubscription extends AtomicLong 
    implements Subscription, Runnable {
        /** */
        private static final long serialVersionUID = -8837830104110774774L;
        final Subscriber<? super Long> actual;
        long currentValue;
        
        volatile Resource resource;
        static final AtomicReferenceFieldUpdater<PeriodicSubscription, Resource> RESOURCE =
                AtomicReferenceFieldUpdater.newUpdater(PeriodicSubscription.class, Resource.class, "resource");

        static final Resource CANCELLED = () -> { };
        
        public PeriodicSubscription(Subscriber<? super Long> actual) {
            this.actual = actual;
        }
        
        public void setResource(Resource res) {
            for (;;) {
                Resource r = resource;
                if (r == CANCELLED) {
                    return;
                }
                if (RESOURCE.compareAndSet(this, r, res)) {
                    if (r != null) {
                        r.close();
                    }
                    return;
                }
            }
        }
        
        @Override
        public void request(long n) {
            if (n <= 0) {
                new IllegalArgumentException("n > 0 required").printStackTrace();
                return;
            }
            RequestManager.add(this, n);
        }
        @Override
        public void cancel() {
            Resource r = resource;
            if (r != CANCELLED) {
                r = RESOURCE.getAndSet(this, CANCELLED);
                if (r != CANCELLED && r != null) {
                    r.close();
                }
            }
        }
        
        public boolean isCancelled() {
            return resource == CANCELLED;
        }
        
        @Override
        public void run() {
            if (isCancelled()) {
                return;
            }
            long r = get();
            if (r != 0L) {
                actual.onNext(currentValue++);
                if (r != Long.MAX_VALUE) {
                    decrementAndGet();
                }
            } else {
                cancel();
                actual.onError(new IllegalStateException("Couldn't emit " + currentValue + " due to the lack of requests."));
            }
        }
    }
}
