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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.akarnokd.rs.impl.queue.MpscLinkedQueue;
import com.github.akarnokd.rs.impl.util.*;

/**
 *
 */
public final class CompositeResource extends AtomicInteger implements ResourceCollection {
    /** */
    private static final long serialVersionUID = 4580690126898626353L;
    
    final float loadFactor;
    
    static final Resource CLOSE = () -> { };
    
    static final Resource REMOVE_ALL = () -> { };

    static final Resource DELETE_ALL = () -> { };

    OpenHashSet<Resource> resources;
    
    final Queue<Resource> queue;

    public CompositeResource() {
        this(16, 0.75f);
    }
    
    public CompositeResource(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        this.resources = new OpenHashSet<>(capacity, loadFactor);
        this.queue = new MpscLinkedQueue<>();
    }
    
    @Override
    public void add(Resource resource) {
        Util.fastpath(this, () -> {
            OpenHashSet<Resource> r = resources;
            if (r == null) {
                resource.close();
                return;
            }
            r.add(resource);
        }, () -> {
            queue.offer(resource);
        }, this::drain);
    }
    
    @Override
    public void remove(Resource resource) {
        Util.fastpath(this, () -> {
            OpenHashSet<Resource> r = resources;
            if (r == null) {
                return;
            }
            if (r.remove(resource)) {
                resource.close();
            }
        }, () -> {
            queue.offer(new DeleteResource(resource, true));
        }, this::drain);
    }
    
    @Override
    public void delete(Resource resource) {
        Util.fastpath(this, () -> {
            OpenHashSet<Resource> r = resources;
            if (r == null) {
                return;
            }
            r.remove(resource);
        }, () -> {
            queue.offer(new DeleteResource(resource, false));
        }, this::drain);
    }
    
    void drain() {
        OpenHashSet<Resource> r = resources;
        final Queue<Resource> q = queue;

        do {
            Resource res = q.poll();
            if (r == null) {
                res.close();
            } else {
                if (res == CLOSE) {
                    resources = null;
                    r.clear(Resource::close);
                    r = null;
                } else
                if (res == REMOVE_ALL) {
                    r.clear(Resource::close);
                } else
                if (res == DELETE_ALL) {
                    r.clear(z -> { });
                } else
                if (res instanceof DeleteResource) {
                    DeleteResource dres = (DeleteResource) res;
                    Resource a = dres.actual;
                    if (r.remove(a) && dres.close) {
                        a.close();
                    }
                } else {
                    r.add(res);
                }
            }
            
        } while (decrementAndGet() != 0);
        
    }
    
    @Override
    public void close() {
        Util.fastpathIf(this, () -> {
            final OpenHashSet<Resource> r = resources;
            if (r != null) {
                resources = null;
                r.clear(Resource::close);
            }
            return true;
        }, () -> {
            queue.offer(CLOSE);
            return true;
        }, this::drain);
    }
    
    @Override
    public void clear() {
        Util.fastpath(this, () -> {
            final OpenHashSet<Resource> r = resources;
            if (r != null) {
                r.clear(Resource::close);
            }
        }, () -> {
            queue.offer(REMOVE_ALL);
        }, this::drain);
    }
}

final class DeleteResource implements Resource {
    final Resource actual;
    final boolean close;
    public DeleteResource(Resource actual, boolean close) {
        this.actual = actual;
        this.close = close;
    }
    @Override
    public void close() {
        actual.close();
    }
}
