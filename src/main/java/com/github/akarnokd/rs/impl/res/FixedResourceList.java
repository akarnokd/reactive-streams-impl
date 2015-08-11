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

/**
 * Contains a fixed number of slots for other Resources.
 * <p>Warning: the class exposes methods of AtomicReferenceArray due to necessity
 * and one shouldn't call these methods. 
 */
public final class FixedResourceList extends AtomicReferenceArray<Resource> implements Resource {
    /** */
    private static final long serialVersionUID = -71328743767168414L;
    
    static final Resource CLOSED = () -> { };
    
    public FixedResourceList(int capacity) {
        super(verify(capacity));
    }
    
    public FixedResourceList(Resource... resources) {
        this(resources.length);
        int n = resources.length;
        int n1 = n - 1;
        for (int i = 0; i < n; i++) {
            lazySet(i, resources[i]);
        }
        set(n1, resources[n1]);
    }
    
    static final int verify(int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("At least capacity of 2 is expected!");
        }
        return capacity;
    }
    /**
     * Sets a slot once and throws IllegalStateException otherwise.
     * @param index
     * @param resource
     * @see #replaceResource(int, Resource)
     */
    public void setResource(int index, Resource resource) {
        if (!compareAndSet(index, null, resource)) {
            if (get(index) != CLOSED) {
                throw new IllegalStateException("Slot " + index + " already set!");
            }
        }
    }
    public void replaceResource(int index, Resource resource) {
        for (;;) {
            Resource r = get(index);
            if (r == CLOSED) {
                resource.close();
                return;
            }
            if (compareAndSet(index, r, resource)) {
                if (r != null) {
                    r.close();
                }
                return;
            }
        }
    }
    
    @Override
    public void close() {
        for (int i = 0; i < length(); i++) {
            Resource r = get(i);
            if (r != CLOSED) {
                r = getAndSet(i, CLOSED);
                if (r != CLOSED) {
                    r.close();
                }
            }
        }
    }
}
