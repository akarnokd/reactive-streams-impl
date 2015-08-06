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

package com.github.akarnokd.rs.impl.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class OpenHashSetTest {
    @Test
    public void addRemove() {
        for (int i = 1; i < 257; i++) {
            OpenHashSet<Integer> set = new OpenHashSet<>(i, 0.75f);
            
            for (int j = 0; j < i; j++) {
                assertTrue(set.add(j));
                assertFalse(set.add(j));
                assertTrue(set.remove(j));
                assertFalse(set.remove(j));
            }
            
            for (int j = 0; j < i; j++) {
                assertTrue(set.add(j));
            }
            for (int j = 0; j < i; j++) {
                assertTrue(set.remove(j));
            }
            
            for (int j = 0; j < i; j++) {
                assertTrue(set.add(j));
            }
            set.clear(e -> { });
            
            for (int j = 0; j < i; j++) {
                assertFalse(set.remove(j));
            }
        }
    }
}
