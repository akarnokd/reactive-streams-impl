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

import java.util.concurrent.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import com.github.akarnokd.rs.impl.res.Resource;
import com.github.akarnokd.rs.impl.subs.*;

/**
 * 
 */
public final class TimerSource implements Publisher<Long> {
    final Supplier<? extends ScheduledExecutorService> schedulerProvider;
    final long delay;
    final TimeUnit unit;
    public TimerSource(long delay, TimeUnit unit, Supplier<? extends ScheduledExecutorService> schedulerProvider) {
        this.delay = delay;
        this.unit = unit;
        this.schedulerProvider = schedulerProvider;
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        ScalarAsyncSubscription<Long> sas = new ScalarAsyncSubscription<>(s);
        SingleResourceSubscription srs = new SingleResourceSubscription(sas);
        s.onSubscribe(srs);
        
        ScheduledExecutorService exec;
        try {
            exec = schedulerProvider.get();
        } catch (Throwable e) {
            s.onError(e);
            return;
        }
        if (exec == null) {
            s.onError(new NullPointerException());
            return;
        }
        
        Future<?> f;
        try {
            f = exec.schedule(() -> sas.setValue(0L), delay, unit);
        } catch (RejectedExecutionException e) {
            s.onError(e);
            return;
        }
        srs.setResource(Resource.from(f));
    }
}
