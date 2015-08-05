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

package com.github.akarnokd.rs.impl.subs;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;

import org.reactivestreams.Subscription;

/**
 * 
 */
public final class SubscriptionArbiter extends AtomicInteger implements Subscription {
    /** */
    private static final long serialVersionUID = -2189523197179400958L;
    Subscription actual;
    long requested;
    
    volatile boolean cancelled;
    volatile long missedRequested;
    volatile long missedProduced;
    final Queue<Subscription> missedSubscription = new ConcurrentLinkedQueue<>();
    
    static final AtomicLongFieldUpdater<SubscriptionArbiter> MISSED_REQUESTED =
            AtomicLongFieldUpdater.newUpdater(SubscriptionArbiter.class, "missedRequested");
    static final AtomicLongFieldUpdater<SubscriptionArbiter> MISSED_PRODUCED =
            AtomicLongFieldUpdater.newUpdater(SubscriptionArbiter.class, "missedProduced");

    private long addRequested(long n) {
        long r = requested;
        long u = r + n;
        if (u < 0L) {
            u = Long.MAX_VALUE;
        }
        requested = u;
        return r;
    }
    
    @Override
    public void request(long n) {
        if (n <= 0) {
            new IllegalArgumentException("n > 0 required").printStackTrace();
            return;
        }
        RequestManager.fastpathLoop(this, () -> {
            addRequested(n);
            Subscription s = actual;
            if (s != null) {
                s.request(n);
            }
        }, () -> {
            RequestManager.add(MISSED_REQUESTED, this, n);
        }, this::drain);
    }

    public void produced(long n) {
        if (n <= 0) {
            new IllegalArgumentException("n > 0 required").printStackTrace();
            return;
        }
        RequestManager.fastpathLoop(this, () -> {
            long r = requested;
            long u = r - n;
            if (u < 0L) {
                new IllegalArgumentException("More produced than requested").printStackTrace();
                u = 0;
            }
            requested = u;
        }, () -> {
            RequestManager.add(MISSED_REQUESTED, this, n);
        }, this::drain);
    }
    
    public void setSubscription(Subscription s) {
        Objects.requireNonNull(s);
        RequestManager.fastpathLoop(this, () -> {
            Subscription a = actual;
            if (a != null) {
                a.cancel();
            }
            actual = s;
            s.request(requested);
        }, () -> {
            missedSubscription.offer(s);
        }, this::drain);
    }
    
    @Override
    public void cancel() {
        if (cancelled) {
            return;
        }
        cancelled = true;
        RequestManager.fastpath(this, () -> {
            Subscription a = actual;
            if (a != null) {
                actual = null;
                a.cancel();
            }
        }, () -> {
            // nothing to queue
        }, this::drain);
    }
    
    void drain() {
        long mr = MISSED_REQUESTED.getAndSet(this, 0L);
        long mp = MISSED_PRODUCED.getAndSet(this, 0L);
        Subscription ms = missedSubscription.poll();
        boolean c = cancelled;
        
        long r = requested;
        if (r != Long.MAX_VALUE && !c) {
            long u = r + mr;
            if (u < 0L) {
                r = Long.MAX_VALUE;
                requested = Long.MAX_VALUE;
            } else {
                long v = u - mp;
                if (v < 0L) {
                    new IllegalStateException("More produced than requested!").printStackTrace();
                    v = 0L;
                }
                r = v;
                requested = v;
            }
        }

        Subscription a = actual;
        if (c && a != null) {
            actual = null;
            a.cancel();
        }
        
        if (ms == null) {
            if (a != null && mr != 0L) {
                a.request(mr);
            }
        } else {
            if (c) {
                ms.cancel();
            } else {
                if (a != null) {
                    a.cancel();
                }
                actual = ms;
                if (r != 0L) {
                    ms.request(r);
                }
            }
        }
    }
}
