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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.*;

/**
 * 
 */
public final class ListSubscriber<T> extends AtomicReference<List<T>> implements Subscriber<T> {
    /** */
    private static final long serialVersionUID = 8141330968948828393L;
    final List<T> values;
    private AtomicReference<Throwable> error = new AtomicReference<>();
    private CountDownLatch cdl = new CountDownLatch(1);
    Subscription s;
    public ListSubscriber() {
        values = new ArrayList<>();
    }
    public ListSubscriber(int capacityHint) {
        values = new ArrayList<>(capacityHint);
    }
    @Override
    public void onSubscribe(Subscription s) {
        this.s = s;
        s.request(Long.MAX_VALUE);
    }
    @Override
    public void onNext(T t) {
        values.add(t);
    }
    @Override
    public void onError(Throwable t) {
        error.lazySet(t);
        cdl.countDown();
    }
    @Override
    public void onComplete() {
        lazySet(values);
        cdl.countDown();
    }
    public List<T> getList() {
        if (cdl.getCount() == 0) {
            return getOrThrow();
        }
        try {
            cdl.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw Throwables.throwIf(ex);
        }
        return getOrThrow();
    }
    public List<T> getList(long time, TimeUnit unit) {
        if (cdl.getCount() == 0) {
            return getOrThrow();
        }
        try {
            if (!cdl.await(time, unit)) {
                throw Throwables.throwIf(new TimeoutException());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw Throwables.throwIf(ex);
        }
        return getOrThrow();
    }
    public List<T> getListNow() {
        Throwables.throwIf(error.get());
        return get(); 
    }
    private List<T> getOrThrow() {
        Throwables.throwIf(error.get());
        List<T> v = get();
        if (v == null) {
            throw new NoSuchElementException();
        }
        return v;
    }
}
