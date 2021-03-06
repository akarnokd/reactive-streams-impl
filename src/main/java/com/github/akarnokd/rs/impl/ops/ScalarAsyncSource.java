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

import java.util.concurrent.Callable;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.ScalarAsyncSubscription;

/**
 *
 */
public final class ScalarAsyncSource<T> implements Publisher<T> {
    final Callable<? extends T> callable;
    public ScalarAsyncSource(Callable<? extends T> callable) {
        this.callable = callable;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        ScalarAsyncSubscription<T> sub = new ScalarAsyncSubscription<>(s);
        s.onSubscribe(sub);
        if (sub.isComplete()) {
            return;
        }
        T value;
        try {
            value = callable.call();
        } catch (Throwable e) {
            if (!sub.isComplete()) {
                s.onError(e);
            }
            return;
        }
        if (sub.isComplete()) {
            return;
        }
        sub.setValue(value);
    }
}
