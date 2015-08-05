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

import java.util.concurrent.Future;

import org.reactivestreams.Subscription;

/**
 * 
 */
public interface Resource extends AutoCloseable {
    @Override
    void close();
    public static Resource from(Subscription s) {
        return s::cancel;
    }
    public static Resource from(Future<?> f) {
        return from(f, true);
    }
    public static Resource from(Future<?> f, boolean interruptFlag) {
        if (interruptFlag) {
            return () -> f.cancel(true);
        }
        return () -> f.cancel(false);
    }
}
