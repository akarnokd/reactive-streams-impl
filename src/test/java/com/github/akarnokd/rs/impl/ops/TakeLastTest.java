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

import org.junit.*;
import org.reactivestreams.Publisher;

import com.github.akarnokd.rs.Publishers;

public class TakeLastTest {
    @Test
    public void simple() {
        Publisher<Integer> source = Publishers.range(1, 10);
        Publisher<Integer> result = Publishers.takeLast(source, 5);
        
        List<Integer> list = Publishers.getList(result);
        
        Assert.assertEquals(Arrays.asList(6, 7, 8, 9, 10), list);
    }
    
    @Test
    public void skipMore() {
        Publisher<Integer> source = Publishers.range(1, 10);
        Publisher<Integer> result = Publishers.takeLast(source, 15);
        
        List<Integer> list = Publishers.getList(result);
        
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), list);
    }

}
