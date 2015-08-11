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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.RequestManager;

/**
 * 
 */
public final class FusedRangeSourceFilter implements Publisher<Integer>, FilterFuseable<Integer> {
    final int start;
    final int end;
    final Predicate<? super Integer> predicate;
    
    public FusedRangeSourceFilter(Predicate<? super Integer> predicate, int start, int count) {
        this.predicate = predicate;
        this.start = start;
        this.end = start + (count - 1);
    }
    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        s.onSubscribe(new FusedRangeFilterSubscription(s, start, end, predicate));
    }
    
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Publisher<Integer> fuse(Predicate<? super Integer> predicate) {
        return new FusedRangeSourceFilter(this.predicate.and((Predicate)predicate), start, end - start + 1);
    }

    static final class FusedRangeFilterSubscription extends AtomicLong implements Subscription {
        /** */
        private static final long serialVersionUID = 7600071995978874818L;
        final int end;
        final Subscriber<? super Integer> actual;
        final Predicate<? super Integer> predicate;

        long index;
        volatile boolean cancelled;
        
        public FusedRangeFilterSubscription(Subscriber<? super Integer> actual, 
                int start, int end, Predicate<? super Integer> predicate) {
            this.actual = actual;
            this.index = start;
            this.end = end;
            this.predicate = predicate;
        }
        @Override
        public void request(long n) {
            if (n == Long.MAX_VALUE && compareAndSet(0L, Long.MAX_VALUE)) {
                fastpath();
            } else
            if (n > 0 && RequestManager.add(this, n) == 0L) {
                slowpath(n);
            } else {
                new IllegalArgumentException("request > 0 required").printStackTrace();
            }
        }
        
        void fastpath() {
            final long end = this.end + 1L;
            final Subscriber<? super Integer> actual = this.actual;
            final Predicate<? super Integer> p = predicate;
            for (long i = index; i != end; i++) {
                if (cancelled) {
                    return;
                }
                Integer v = (int)i;
                boolean b;
                try {
                    b = p.test(v);
                } catch (Throwable t) {
                    actual.onError(t);
                    return;
                }
                
                if (b) {
                    actual.onNext(v);
                }
            }
            if (!cancelled) {
                actual.onComplete();
            }
        }
        
        void slowpath(long r) {
            long idx = index;
            
            for (;;) {
                final long end = this.end + 1L;
                long fs = end - idx;
                long e = Math.min(fs, r);
                final boolean complete = fs <= r;

                fs = e + idx;
                final Subscriber<? super Integer> actual = this.actual;
                
                final Predicate<? super Integer> p = predicate;
                
                for (long i = idx; i != fs && i != end; i++) {
                    if (cancelled) {
                        return;
                    }
                    Integer v = (int) i;
                    boolean b;
                    try {
                        b = p.test(v);
                    } catch (Throwable t) {
                        actual.onError(t);
                        return;
                    }
                    
                    if (b) {
                        actual.onNext(v);
                    } else {
                        fs++;
                    }
                }
                
                if (complete) {
                    if (!cancelled) {
                        actual.onComplete();
                    }
                    return;
                }
                
                idx = fs;
                index = fs;
                
                r = addAndGet(-r);
                if (r == 0L) {
                    return;
                }
            }
        }
        
        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
