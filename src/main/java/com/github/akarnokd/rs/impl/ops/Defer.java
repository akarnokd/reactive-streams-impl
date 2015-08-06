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

import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.EmptySubscription;

/**
 *
 */
public final class Defer<T> implements Publisher<T> {
    final Supplier<? extends Publisher<? extends T>> supplier;
    public Defer(Supplier<? extends Publisher<? extends T>> supplier) {
        this.supplier = supplier;
    }
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T> pub;
        try {
            pub = supplier.get();
        } catch (Throwable t) {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onError(t);
            return;
        }
        
        pub.subscribe(s);
    }
}
