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
package com.github.akarnokd.rs;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.impl.ops.*;

/**
 * Contains operators to work with reactive-streams {@link Publisher}s non-fluently.
 */
public enum Publishers {
    ; // Singleton
    /** Default backpressure buffer size. */
    private static int BUFFER_SIZE;
    static {
        BUFFER_SIZE = Integer.getInteger("rsi.buffer-size", 256);
    }
    public static int bufferSize() {
        return BUFFER_SIZE;
    }
    
    public static <T> Publisher<T> just(T value) {
        Objects.requireNonNull(value);
        return new ScalarSource<>(value);
    }
    
    public static <T, U> Publisher<U> flatMap(Publisher<T> source, 
            Function<T, Publisher<U>> mapper) {
        return flatMap(source, mapper, Integer.MAX_VALUE, false, bufferSize());
    }
    
    public static <T, U> Publisher<U> flatMap(Publisher<? extends T> source, 
            Function<? super T, ? extends Publisher<U>> mapper, int maxSubscription, boolean delayErrors, int bufferSize) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(mapper);
        if (maxSubscription <= 0) {
            throw new IllegalArgumentException("maxSubscription > 0 required");
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize > 0 required");
        }
        if (source instanceof ScalarSource) {
            return ((ScalarSource<? extends T>)source).flatMap(mapper);
        }
        // TODO
        throw new UnsupportedOperationException();
    }
    
    public static <T> T getScalar(Publisher<? extends T> source) {
        Objects.requireNonNull(source);
        ScalarSubscriber<T> s = new ScalarSubscriber<>();
        source.subscribe(s);
        return s.getValue();
    }
    public static <T> T getScalarNow(Publisher<? extends T> source) {
        Objects.requireNonNull(source);
        ScalarSubscriber<T> s = new ScalarSubscriber<>();
        source.subscribe(s);
        return s.getNow();
    }
    
    public static <T> Publisher<T> take(Publisher<? extends T> source, long limit) {
        if (limit < 0L) {
            throw new IllegalArgumentException("limit >= required");
        }
        Objects.requireNonNull(source);
        if (source instanceof Take) {
            @SuppressWarnings("unchecked")
            Take<T> tp = (Take<T>)source;
            if (tp.limit() <= limit) {
                return tp;
            }
            return new Take<>(tp.source(), limit);
        }
        return new Take<>(source, limit);
    }
    @SafeVarargs
    public static <T> Publisher<T> fromArray(T... values) {
        Objects.requireNonNull(values);
        return new ArraySource<>(values);
    }
    public static <T> List<T> getList(Publisher<? extends T> source) {
        Objects.requireNonNull(source);
        ListSubscriber<T> s = new ListSubscriber<>();
        source.subscribe(s);
        return s.getList();
    }
    public static <T> List<T> getListNow(Publisher<? extends T> source) {
        Objects.requireNonNull(source);
        ListSubscriber<T> s = new ListSubscriber<>();
        source.subscribe(s);
        return s.getList();
    }
    
    public static <T> Publisher<T> fromStream(Stream<? extends T> source) {
        Objects.requireNonNull(source);
        return new StreamSource<>(source);
    }
    
    public static <T> Publisher<T> fromIterable(Iterable<? extends T> source) {
        Objects.requireNonNull(source);
        return new IterableSource<>(source);
    }
    
    public static <T> Publisher<T> fromCallable(Callable<? extends T> callable) {
        Objects.requireNonNull(callable);
        return new ScalarAsyncSource<>(callable);
    }
    /**
     * <p>Cancelling the subscription won't cancel the future.
     * @param <T> the value type
     * @param future the future to turn into a Publisher
     * @return the Publisher emitting the result value or exception from the given future
     */
    public static <T> Publisher<T> fromFuture(CompletableFuture<? extends T> future) {
        Objects.requireNonNull(future);
        return new CompletableFutureSource<>(future);
    }
    
    public static <T> Publisher<T> subscribeOn(Publisher<? extends T> source, ExecutorService executor) {
        // direct chaining of subscribeOn is a waste
        Objects.requireNonNull(source);
        Objects.requireNonNull(executor);
        return subscribeOn0(source, () -> executor);
    }
    public static <T> Publisher<T> subscribeOn(Publisher<? extends T> source, Supplier<ExecutorService> executorSupplier) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(executorSupplier);
        return subscribeOn0(source, executorSupplier);
    }
    @SuppressWarnings("unchecked")
    private static <T> Publisher<T> subscribeOn0(Publisher<? extends T> source, Supplier<ExecutorService> executorSupplier) {
        if (source instanceof SubscribeOn) {
            return (Publisher<T>)source;
        }
        return new SubscribeOn<>(source, executorSupplier);
        
    }
    
    public static <T> Publisher<T> defer(Supplier<? extends Publisher<? extends T>> publisherSupplier) {
        return new Defer<>(publisherSupplier);
    }
    
    public static <T> Publisher<T> observeOn(Publisher<? extends T> source, ExecutorService executor) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(executor);
        return observeOn0(source, () -> executor, bufferSize(), false);
    }
    
    public static <T> Publisher<T> observeOn(Publisher<? extends T> source, ExecutorService executor, int bufferSize) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(executor);
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize > 0 required");
        }
        return observeOn0(source, () -> executor, bufferSize, false);
    }

    public static <T> Publisher<T> observeOn0(Publisher<? extends T> source, Supplier<ExecutorService> executorSupplier, int bufferSize, boolean delayError) {
        return new ObserveOn<>(source, executorSupplier, bufferSize, delayError);
    }
    
    public static <T, U> Publisher<U> map(Publisher<? extends T> source, Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(mapper);
        if (source instanceof Mapper) {
            @SuppressWarnings("unchecked")
            Mapper<Object, T> m = (Mapper<Object, T>)source;
            return new Mapper<>(m.source(), m.function().andThen(mapper));
        }
        return new Mapper<>(source, mapper);
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Publisher<T> filter(Publisher<? extends T> source, Predicate<? super T> filter) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(filter);
        if (source instanceof Filter) {
            Filter<? extends T> m = (Filter<? extends T>)source;
            return new Filter(m.source(), m.filter().and((Predicate)filter));
        }
        return new Filter<>(source, filter);
    }
}
