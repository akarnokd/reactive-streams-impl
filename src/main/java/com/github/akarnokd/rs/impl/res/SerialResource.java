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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 */
public final class SerialResource extends AtomicReference<Resource> implements Resource {
    /** */
    private static final long serialVersionUID = 2058907430530075041L;
    
    static final Resource CLOSED = () -> { };

    static final Resource EMPTY = () -> { };
    
    public SerialResource() {
        this(EMPTY);
    }
    public SerialResource(Resource res) {
        lazySet(res);
    }
    public void setNext(Resource next) {
        Objects.requireNonNull(next);
        for (;;) {
            Resource ac = get();
            if (ac == CLOSED) {
                next.close();
                return;
            }
            if (compareAndSet(ac, next)) {
                ac.close();
                return;
            }
        }
    }
    @Override
    public void close() {
        Resource ac = get();
        if (ac != CLOSED) {
            ac = getAndSet(CLOSED);
            if (ac != CLOSED) {
                ac.close();
            }
        }
    }
}
