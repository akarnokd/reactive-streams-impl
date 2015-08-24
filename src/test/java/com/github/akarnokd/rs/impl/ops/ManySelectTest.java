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

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

/**
 *
 */
public class ManySelectTest {
    @Test
    public void simple() {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Publisher<Integer> source = Publishers.range(1, 10);
            
            Publisher<Integer> result = Publishers.merge(
                    Publishers.manySelect(source, p -> Publishers.sumInteger(p), exec)
            );
            
            List<Integer> list = Publishers.getList(result);
            Assert.assertEquals(Arrays.asList(55, 54, 52, 49, 45, 40, 34, 27, 19, 10), list);
        } finally {
            exec.shutdownNow();
        }
    }
}
