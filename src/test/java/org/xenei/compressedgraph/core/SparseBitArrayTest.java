/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.compressedgraph.core;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.xenei.compressedgraph.core.SparseBitArray;

public class SparseBitArrayTest {

	private SparseBitArray ary;

	@Before
	public void beforeSparseArrayTest() {
		ary = new SparseBitArray();

	}

	@Test
	public void testPutGet() {
		ary.set(1);
		ary.set(10);
		ary.set(1000);
		ary.set(1000000);
		ary.set(Integer.MAX_VALUE);

		assertTrue(ary.get(1));
		assertTrue(ary.get(10));
		assertTrue(ary.get(1000));
		assertTrue(ary.get(1000000));
		assertTrue(ary.get(Integer.MAX_VALUE));
		assertFalse(ary.get(5));
	}

	@Test
	public void testPutBeforeFirstGet() {
		ary.set(Integer.MAX_VALUE);
		ary.set(1);
		ary.set(10);
		ary.set(1000);
		ary.set(1000000);

		assertTrue(ary.get(1));
		assertTrue(ary.get(10));
		assertTrue(ary.get(1000));
		assertTrue(ary.get(1000000));
		assertTrue(ary.get(Integer.MAX_VALUE));
		assertFalse(ary.get(5));
	}

	@Test
	public void testIndexIterator() {
		ary.set(Integer.MAX_VALUE);
		ary.set(1);
		ary.set(10);
		ary.set(1000);
		ary.set(1000000);

		Iterator<Integer> iter = ary.iterator();

		assertEquals(Integer.valueOf(1), iter.next());
		assertEquals(Integer.valueOf(10), iter.next());
		assertEquals(Integer.valueOf(1000), iter.next());
		assertEquals(Integer.valueOf(1000000), iter.next());
		assertEquals(Integer.valueOf(Integer.MAX_VALUE), iter.next());
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		} catch (NoSuchElementException expected) {
			// expected
		}
	}

	@Test
	public void testClear() {
		ary.set(Integer.MAX_VALUE);
		ary.set(1);
		ary.set(10);
		ary.set(1000);
		ary.set(1000000);

		assertFalse(ary.isEmpty());
		ary.clear(1000);

		Iterator<Integer> iter = ary.iterator();

		assertEquals(Integer.valueOf(1), iter.next());
		assertEquals(Integer.valueOf(10), iter.next());
		assertEquals(Integer.valueOf(1000000), iter.next());
		assertEquals(Integer.valueOf(Integer.MAX_VALUE), iter.next());
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		} catch (NoSuchElementException expected) {
			// expected
		}

		ary.clear(Integer.MAX_VALUE);

		iter = ary.iterator();

		assertEquals(Integer.valueOf(1), iter.next());
		assertEquals(Integer.valueOf(10), iter.next());
		assertEquals(Integer.valueOf(1000000), iter.next());
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		} catch (NoSuchElementException expected) {
			// expected
		}

		ary.clear(1);
		iter = ary.iterator();

		assertEquals(Integer.valueOf(10), iter.next());
		assertEquals(Integer.valueOf(1000000), iter.next());
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		} catch (NoSuchElementException expected) {
			// expected
		}

		ary.clear(10);
		iter = ary.iterator();

		assertEquals(Integer.valueOf(1000000), iter.next());
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		} catch (NoSuchElementException expected) {
			// expected
		}

		ary.clear(1000000);

		assertFalse(ary.iterator().hasNext());
		assertTrue(ary.isEmpty());

	}
}
