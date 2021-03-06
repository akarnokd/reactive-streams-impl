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

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.subs.EmptySubscription;

/**
 * 
 */
public enum EmptyPublisher implements Publisher<Object>{
    INSTANCE;
    @SuppressWarnings("unchecked")
    public static <T> Publisher<T> empty() {
        return (Publisher<T>)INSTANCE;
    }
    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
        s.onComplete();
    }
}
