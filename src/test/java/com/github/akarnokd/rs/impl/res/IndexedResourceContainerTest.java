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

import org.junit.*;

import com.github.akarnokd.rs.Publishers;

/**
 * 
 */
public class IndexedResourceContainerTest {
    @Test
    public void addRemoveOneByOne() {
        System.out.println("addRemoveOneByOne");
        try (IndexedResourceContainer<Integer> c = new IndexedResourceContainer<>(Publishers.bufferSize(), e -> { })) {
            for (int i = 0; i < 1_000_000; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                Assert.assertTrue(c.allocate(i));
                Assert.assertTrue(c.setResource(i, i));
                c.deleteResource(i);
            }
        }
    }
    
    @Test
    public void addAllRemoveAll() {
        System.out.println("addAllRemoveAll");
        try (IndexedResourceContainer<Integer> c = new IndexedResourceContainer<>(Publishers.bufferSize(), e -> { })) {
            for (int i = 0; i < 1_000_000; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                Assert.assertTrue(c.allocate(i));
                Assert.assertTrue(c.setResource(i, i));
            }
            for (int i = 0; i < 1_000_000; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                c.deleteResource(i);
            }
        }
    }
    @Test
    public void addAllRemoveAllReverse() {
        System.out.println("addAllRemoveAllReverse");
        try (IndexedResourceContainer<Integer> c = new IndexedResourceContainer<>(Publishers.bufferSize(), e -> { })) {
            int n = 10_000;
            for (int i = 0; i < n; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                Assert.assertTrue(c.allocate(i));
                Assert.assertTrue(c.setResource(i, i));
            }
            for (int i = n - 1; i >= 0; i--) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                c.deleteResource(i);
            }
        }
    }
    @Test
    public void cancelAll() {
        int[] counter = { 0 };
        int n = 1_000_000;
        try (IndexedResourceContainer<Integer> c = new IndexedResourceContainer<>(Publishers.bufferSize(), e -> counter[0]++)) {
            for (int i = 0; i < n; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                Assert.assertTrue(c.allocate(i));
                Assert.assertTrue(c.setResource(i, i));
            }
            c.close();
            Assert.assertEquals(n, counter[0]);
            Assert.assertFalse(c.allocate(n));
        }
    }
    
    @Test
    public void removeHalfCancelHalf() {
        int[] counter = { 0 };
        int n = 1_000_000;
        try (IndexedResourceContainer<Integer> c = new IndexedResourceContainer<>(Publishers.bufferSize(), e -> counter[0]++)) {
            for (int i = 0; i < n; i++) {
                if (i % 1000 == 0) {
                    System.out.printf("--> %d%n", i);
                }
                Assert.assertTrue(c.allocate(i));
                Assert.assertTrue(c.setResource(i, i));
            }
            
            int m = n - n / 2;
            for (int i = 0; i < n / 2; i++) {
                c.deleteResource(i);
            }
            c.close();
            Assert.assertEquals(m, counter[0]);
            Assert.assertFalse(c.allocate(n));
        }
    }
}
