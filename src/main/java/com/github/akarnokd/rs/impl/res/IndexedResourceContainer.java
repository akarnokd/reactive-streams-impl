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

import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

import com.github.akarnokd.rs.impl.res.IndexedResourceContainer.ARA;

/**
 * 
 */
public final class IndexedResourceContainer<T> extends AtomicReference<ARA> implements Resource {
    /** */
    private static final long serialVersionUID = -1192054364934225081L;

    final Consumer<? super T> closer;
    final int capacity;
    
    volatile ARA tail;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<IndexedResourceContainer, ARA> TAIL =
            AtomicReferenceFieldUpdater.newUpdater(IndexedResourceContainer.class, ARA.class, "tail");
    
    static final Object TOMBSTONE = new Object();
    static final Object CLOSED_ITEM = new Object();
    static final ARA CLOSED_ARA = new ARA(Long.MIN_VALUE, 0);
    
    public IndexedResourceContainer(int elementCapacity, Consumer<? super T> closer) {
        this.closer = closer;
        int c = elementCapacity + 1;
        this.capacity = c;
        ARA ara = new ARA(0L, c);
        TAIL.lazySet(this, ara);
        lazySet(ara);
    }
    
    
    public boolean allocate(long index) {
        ARA a = get();
        if (a == CLOSED_ARA) {
            return false;
        }
        long end = a.endIndex;
        if (end > index) {
            return true;
        }
        int c = capacity;
        if (end + c - 1 <= index) {
            throw new UnsupportedOperationException(String.format("Allocating more than one segment! End: %d, Index: %d", end, index));
        }
        ARA b = new ARA(end, c);
        if (a.casNext(null, b)) {
            return compareAndSet(a, b);
        }
        return false;
    }
    
    public boolean setResource(long index, T resource) {
        final Consumer<? super T> closer = this.closer;
        ARA a = get();
        if (a == CLOSED_ARA) {
            closer.accept(resource);
            return false;
        }
        long start = a.startIndex;
        long end = a.endIndex;
        if (start <= index && index < end) {
            int idx = (int)(index - start);
            if (!a.compareAndSet(idx, null, resource)) {
                Object o = a.get(idx);
                if (o == CLOSED_ITEM) {
                    closer.accept(resource);
                }
                return false;
            }
            return true;
        }
        throw new IndexOutOfBoundsException(String.format("Start: %d, End: %d, Index: %d", start, end, index));
    }
    
    public void deleteResource(long index) {
        ARA a = tail;
        if (a == CLOSED_ARA) {
            return;
        }
        long start = a.startIndex;
        if (start > index) {
            throw new IndexOutOfBoundsException(String.format("Start: %d, Index: %d", start, index));
        }
        long end = a.endIndex;
        if (index < end) {
            int idx = (int)(index - start);
            Object o = a.get(idx);
            if (o == CLOSED_ITEM) {
                return;
            }
            if (!a.compareAndSet(idx, o, TOMBSTONE)) {
                o = a.get(idx);
                if (o == CLOSED_ITEM) {
                    return;
                }
            }
            
            if (a.releaseOne() != capacity) {
                return;
            }
        } else {
            for (;;) {
                a = a.lvNext();
                if (a == null) {
                    throw new IllegalStateException("Item at index " + index + " not allocated!");
                } else
                if (a == CLOSED_ARA) {
                    return;
                }
                end = a.endIndex;
                if (index < end) {
                    start = a.startIndex;
                    int idx = (int)(index - start);
                    Object o = a.get(idx);
                    if (o == CLOSED_ITEM) {
                        return;
                    }
                    if (!a.compareAndSet(idx, o, TOMBSTONE)) {
                        o = a.get(idx);
                        if (o == CLOSED_ITEM) {
                            return;
                        }
                    }
                    a.releaseOne();
                    break;
                }
                
            }
        }
        
        final int c = capacity;
        a = tail;
        for (;;) {
            if (a == CLOSED_ARA) {
                break;
            }
            if (a.releaseCount() == c) {
                ARA b = a.lvNext();
                if (b == null || b == CLOSED_ARA) {
                    break;
                }
                if (TAIL.compareAndSet(this, a, b)) {
                    a = b;
                } else {
                    a = tail;
                }
            } else {
                break;
            }
        }
    }
    
    @Override
    public void close() {
        ARA a = get();
        if (a != CLOSED_ARA) {
            a = getAndSet(CLOSED_ARA);
            if (a == CLOSED_ARA) {
                return;
            }
        }
        
        a = tail;
        if (a != CLOSED_ARA) {
            a = TAIL.getAndSet(this, CLOSED_ARA);
            if (a == CLOSED_ARA) {
                return;
            }
        } else {
            return;
        }
        
        final int c1 = capacity - 1;
        final Consumer<? super T> closer = this.closer;
        while (a != null) {
            for (int i = 0; i < c1; i++) {
                Object o = a.getAndSet(i, CLOSED_ITEM);
                if (o != TOMBSTONE && o != null) {
                    @SuppressWarnings("unchecked")
                    T v = (T)o;
                    closer.accept(v);
                }
            }
            
            ARA b = (ARA)a.getAndSet(c1, CLOSED_ARA);
            if (b == null) {
                break;
            }
            a = b;
        }
    }
    
    
    static final class ARA extends AtomicReferenceArray<Object> {
        /** */
        private static final long serialVersionUID = -721629654371317268L;
        final int nextOffset;
        final long startIndex;
        /** Exclusive. */
        final long endIndex;
        
        volatile int releaseCount;
        static final AtomicIntegerFieldUpdater<ARA> RELEASE_COUNT =
                AtomicIntegerFieldUpdater.newUpdater(ARA.class, "releaseCount");
        
        public ARA(long startIndex, int capacity) {
            super(capacity);
            nextOffset = capacity - 1;
            this.releaseCount = 1;
            this.startIndex = startIndex;
            this.endIndex = startIndex + capacity - 1;
        }
        public ARA lvNext() {
            return (ARA)get(nextOffset);
        }
        
        public boolean casNext(ARA expected, ARA value) {
            return compareAndSet(nextOffset, expected, value);
        }
        
        public int releaseOne() {
            return RELEASE_COUNT.incrementAndGet(this);
        }
        
        public int releaseCount() {
            return releaseCount;
        }
    }
}
