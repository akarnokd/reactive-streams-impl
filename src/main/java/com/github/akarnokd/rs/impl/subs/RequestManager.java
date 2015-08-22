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
            long u = addCap(r, n);
            if (updater.compareAndSet(instance, r, u)) {
                return r;
            }
        }
    }
    
    public static long addCap(long a, long b) {
        long u = a + b;
        if (u < 0L) {
            u = Long.MAX_VALUE;
        }
        return u;
    }
    
    public static long multiplyCap(long base, long scale) {
        long u = base * scale;
        if ((((base | scale) >>> 31) != 0) && (u / base != scale)) {
            u = Long.MAX_VALUE;
        }
        return u;
    }
}
