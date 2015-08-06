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

package com.github.akarnokd.rs.impl.queue;

import java.util.*;
import java.util.concurrent.atomic.*;

import com.github.akarnokd.rs.impl.util.Util;

import sun.misc.Contended;

/**
 * 
 */
public final class SpscArrayQueue<T> extends AtomicReferenceArray<T> implements Queue<T> {
    /** */
    private static final long serialVersionUID = 6210984603741293445L;
    final int mask;
    final int capacitySkip;
    @Contended
    volatile long producerIndex;
    @Contended
    volatile long consumerIndex;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscArrayQueue> PRODUCER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscArrayQueue.class, "producerIndex");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscArrayQueue> CONSUMER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscArrayQueue.class, "consumerIndex");
    
    public SpscArrayQueue(int capacity) {
        super(Util.roundUp(capacity));
        int len = length();
        this.mask = len - 1;
        this.capacitySkip = len - capacity; 
    }
    
    
    @Override
    public boolean offer(T value) {
        Objects.requireNonNull(value);
        
        long pi = producerIndex;
        int m = mask;
        
        int fullCheck = (int)(pi + capacitySkip) & m;
        if (get(fullCheck) != null) {
            return false;
        }
        int offset = (int)pi & m;
        PRODUCER_INDEX.lazySet(this, pi + 1);
        lazySet(offset, value);
        return true;
    }
    @Override
    public T poll() {
        long ci = consumerIndex;
        int offset = (int)ci & mask;
        T value = get(offset);
        if (value == null) {
            return null;
        }
        CONSUMER_INDEX.lazySet(this, ci + 1);
        lazySet(offset, null);
        return value;
    }
    @Override
    public T peek() {
        return get((int)consumerIndex & mask);
    }
    @Override
    public void clear() {
        while (poll() != null || !isEmpty());
    }
    @Override
    public boolean isEmpty() {
        return producerIndex == consumerIndex;
    }
    
    @Override
    public int size() {
        long ci = consumerIndex;
        for (;;) {
            long pi = producerIndex;
            long ci2 = consumerIndex;
            if (ci == ci2) {
                return (int)(pi - ci2);
            }
            ci = ci2;
        }
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T element() {
        throw new UnsupportedOperationException();
    }
    
}
