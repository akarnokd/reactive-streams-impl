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
package com.github.akarnokd.rs.impl.res;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

public final class FixedResourceContainer<T> extends AtomicReferenceArray<Object> implements Resource {
    /** */
    private static final long serialVersionUID = 6165384795300816654L;
    final Consumer<? super T> closer;
    private static final Object CLOSED = new Object();
    
    public FixedResourceContainer(int capacity, Consumer<? super T> closer) {
        super(verify(capacity));
        this.closer = closer;
    }
    static int verify(int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("capacity >= 2 required but it was " + capacity);
        }
        return capacity;
    }
    
    public void setResource(int index, T resource) {
        if (!compareAndSet(index, null, resource)) {
            if (get(index) != CLOSED) {
                throw new IllegalStateException("Slot " + index + " already set!");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    public void replaceResource(int index, T resource) {
        for (;;) {
            Object r = get(index);
            if (r == CLOSED) {
                closer.accept(resource);
                return;
            }
            if (compareAndSet(index, r, resource)) {
                if (r != null) {
                    closer.accept((T)r);
                }
                return;
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void close() {
        int s = length();
        for (int i = 0; i < s; i++) {
            Object r = get(i);
            if (r != CLOSED) {
                r = getAndSet(i, CLOSED);
                if (r != CLOSED) {
                    closer.accept((T)r);
                }
            }
        }
    }
}
