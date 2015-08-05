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

import java.util.concurrent.atomic.*;

/**
 * 
 */
public enum RequestManager {
    ;
    public static long add(AtomicLong ref, long n) {
        for (;;) {
            long r = ref.get();
            long u = r + n;
            if (u < 0) {
                u = Long.MAX_VALUE;
            }
            if (ref.compareAndSet(r, u)) {
                return r;
            }
        }
    }
    public static <T> long add(AtomicLongFieldUpdater<T> updater, T instance, long n) {
        for (;;) {
            long r = updater.get(instance);
            long u = r + n;
            if (u < 0) {
                u = Long.MAX_VALUE;
            }
            if (updater.compareAndSet(instance, r, u)) {
                return r;
            }
        }
    }
    
    public static void fastpathLoop(AtomicInteger wip, Runnable fast, 
            Runnable queue, Runnable drain) {
        if (wip.get() == 0 && wip.compareAndSet(0, 1)) {
            fast.run();
            if (wip.decrementAndGet() == 0) {
                return;
            }
        } else {
            queue.run();
            if (wip.getAndIncrement() != 0) {
                return;
            }
        }
        do {
            drain.run();
        } while (wip.decrementAndGet() != 0);
    }
    
    public static <T> void fastpathLoop(AtomicIntegerFieldUpdater<T> updater, T instance, 
            Runnable fast, Runnable queue, Runnable drain) {
        if (updater.get(instance) == 0 && updater.compareAndSet(instance, 0, 1)) {
            fast.run();
            if (updater.decrementAndGet(instance) == 0) {
                return;
            }
        } else {
            queue.run();
            if (updater.getAndIncrement(instance) != 0) {
                return;
            }
        }
        do {
            drain.run();
        } while (updater.decrementAndGet(instance) != 0);
    }
    
    public static void fastpath(AtomicInteger wip, Runnable fast, 
            Runnable queue, Runnable drain) {
        if (wip.get() == 0 && wip.compareAndSet(0, 1)) {
            fast.run();
            if (wip.decrementAndGet() == 0) {
                return;
            }
        } else {
            queue.run();
            if (wip.getAndIncrement() != 0) {
                return;
            }
        }
        drain.run();
    }
    
    public static <T> void fastpath(AtomicIntegerFieldUpdater<T> updater, T instance, 
            Runnable fast, Runnable queue, Runnable drain) {
        if (updater.get(instance) == 0 && updater.compareAndSet(instance, 0, 1)) {
            fast.run();
            if (updater.decrementAndGet(instance) == 0) {
                return;
            }
        } else {
            queue.run();
            if (updater.getAndIncrement(instance) != 0) {
                return;
            }
        }
        drain.run();
    }
}
