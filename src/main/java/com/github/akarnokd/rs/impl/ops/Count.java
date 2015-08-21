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

import com.github.akarnokd.rs.impl.subs.*;

public final class Count implements Publisher<Long> {
    final Publisher<?> source;
    public Count(Publisher<?> source) {
        this.source = source;
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        ScalarAsyncSubscription<Long> sas = new ScalarAsyncSubscription<>(s);
        SingleResourceSubscription srs = new SingleResourceSubscription(sas);
        s.onSubscribe(srs);
        
        source.subscribe(new Subscriber<Object>() {
            long count;
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
                srs.setResource(s::cancel);
            }
            @Override
            public void onNext(Object t) {
                count++;
            }
            @Override
            public void onError(Throwable t) {
                s.onError(t);
            }
            @Override
            public void onComplete() {
                sas.setValue(count);
            }
        });
    }
}
