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

package com.github.akarnokd.rs.impl.util;

import java.util.concurrent.atomic.*;
import java.util.function.*;

public enum Util {
    ;
    public static int roundUp(int capacity) {
        int tz = 32 - Integer.numberOfLeadingZeros(capacity);
        return 1 << tz;
    }
    
    private static final int INT_PHI = 0x9E3779B9;
    
    public static int mix(int x) {
        final int h = x * INT_PHI;
        return h ^ (h >>> 16);
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
    
    public static void fastpathIf(AtomicInteger wip, BooleanSupplier fast, 
            BooleanSupplier queue, Runnable drain) {
        if (wip.get() == 0 && wip.compareAndSet(0, 1)) {
            if (fast.getAsBoolean()) {
                return;
            }
            if (wip.decrementAndGet() == 0) {
                return;
            }
        } else {
            if (queue.getAsBoolean()) {
                return;
            }
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
    
    public static <T> void atomicReplace(AtomicReference<T> ref, T newValue, 
            T terminalValue, Consumer<? super T> oldValue) {
        for (;;) {
            T curr = ref.get();
            if (curr == terminalValue) {
                oldValue.accept(newValue);
                return;
            }
            if (ref.compareAndSet(curr, newValue)) {
                oldValue.accept(curr);
                return;
            }
        }
    }
    public static <T> void atomicTerminate(AtomicReference<T> ref, T terminalValue, 
            Consumer<? super T> oldValue) {
        T curr = ref.get();
        if (curr != terminalValue) {
            curr = ref.getAndSet(terminalValue);
            if (curr != terminalValue) {
                oldValue.accept(curr);
            }
        }
    }
    
    public static <T, U> void atomicReplace(AtomicReferenceFieldUpdater<T, U> updater, T instance, 
            U newValue, U terminalValue, Consumer<? super U> oldValue) {
        for (;;) {
            U curr = updater.get(instance);
            if (curr == terminalValue) {
                oldValue.accept(newValue);
                return;
            }
            if (updater.compareAndSet(instance, curr, newValue)) {
                oldValue.accept(curr);
                return;
            }
        }
    }
    public static <T, U> void atomicTerminate(AtomicReferenceFieldUpdater<T, U> updater, T instance, 
            U terminalValue, Consumer<? super U> oldValue) {
        U curr = updater.get(instance);
        if (curr != terminalValue) {
            curr = updater.getAndSet(instance, terminalValue);
            if (curr != terminalValue) {
                oldValue.accept(curr);
            }
        }
    }
}
