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
import java.util.List;

import org.junit.Test;
import org.xenei.compressedgraph.core.BitCube;
import org.xenei.compressedgraph.core.BitCube.Idx;

public class BitCubeTest {

	private BitCube cube;

	@Test
	public void testSetHas() {
		cube = new BitCube();

		cube.set(1, 1, 1);
		cube.set(1, 1, 2);
		cube.set(1, 2, 3);
		cube.set(10, 10, 10);
		cube.set(10, 10, 20);
		cube.set(10, 20, 30);
		cube.set(100, 100, 100);
		cube.set(100, 100, 200);
		cube.set(100, 200, 300);
		cube.set(Integer.MAX_VALUE, 1, 1);
		cube.set(Integer.MAX_VALUE, 1, 2);
		cube.set(Integer.MAX_VALUE, 2, 3);
		cube.set(Integer.MAX_VALUE, 10, 10);
		cube.set(Integer.MAX_VALUE, 10, 20);
		cube.set(Integer.MAX_VALUE, 20, 30);
		cube.set(Integer.MAX_VALUE, 100, 100);
		cube.set(Integer.MAX_VALUE, 200, 200);
		cube.set(Integer.MAX_VALUE, 200, 300);
		cube.set(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(cube.has(1, 1, 1));
		assertTrue(cube.has(1, 1, 2));
		assertTrue(cube.has(1, 2, 3));
		assertTrue(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		assertFalse(cube.has(11, 11, 1));
		assertFalse(cube.has(1, 12, 12));
		assertFalse(cube.has(11, 2, 3));
		assertFalse(cube.has(110, 10, 10));
	}

	@Test
	public void testClear() {
		cube = new BitCube();

		cube.set(1, 1, 1);
		cube.set(1, 1, 2);
		cube.set(1, 2, 3);
		cube.set(10, 10, 10);
		cube.set(10, 10, 20);
		cube.set(10, 20, 30);
		cube.set(100, 100, 100);
		cube.set(100, 100, 200);
		cube.set(100, 200, 300);

		cube.set(Integer.MAX_VALUE, 1, 1);
		cube.set(Integer.MAX_VALUE, 1, 2);
		cube.set(Integer.MAX_VALUE, 2, 3);
		cube.set(Integer.MAX_VALUE, 10, 10);
		cube.set(Integer.MAX_VALUE, 10, 20);
		cube.set(Integer.MAX_VALUE, 20, 30);
		cube.set(Integer.MAX_VALUE, 100, 100);
		cube.set(Integer.MAX_VALUE, 200, 200);
		cube.set(Integer.MAX_VALUE, 200, 300);
		cube.set(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(cube.has(1, 1, 1));
		assertTrue(cube.has(1, 1, 2));
		assertTrue(cube.has(1, 2, 3));
		assertTrue(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(1, 1, 1);
		assertFalse(cube.has(1, 1, 1));
		assertTrue(cube.has(1, 1, 2));
		assertTrue(cube.has(1, 2, 3));
		assertTrue(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(1, 1, 2);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertTrue(cube.has(1, 2, 3));
		assertTrue(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(1, 2, 3);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertFalse(cube.has(1, 2, 3));
		assertTrue(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(10, 10, 10);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertFalse(cube.has(1, 2, 3));
		assertFalse(cube.has(10, 10, 10));
		assertTrue(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(10, 10, 20);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertFalse(cube.has(1, 2, 3));
		assertFalse(cube.has(10, 10, 10));
		assertFalse(cube.has(10, 10, 20));
		assertTrue(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(10, 20, 30);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertFalse(cube.has(1, 2, 3));
		assertFalse(cube.has(10, 10, 10));
		assertFalse(cube.has(10, 10, 20));
		assertFalse(cube.has(10, 20, 30));
		assertTrue(cube.has(100, 100, 100));
		assertTrue(cube.has(100, 100, 200));
		assertTrue(cube.has(100, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 1));
		assertTrue(cube.has(Integer.MAX_VALUE, 1, 2));
		assertTrue(cube.has(Integer.MAX_VALUE, 2, 3));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 10));
		assertTrue(cube.has(Integer.MAX_VALUE, 10, 20));
		assertTrue(cube.has(Integer.MAX_VALUE, 20, 30));
		assertTrue(cube.has(Integer.MAX_VALUE, 100, 100));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 200));
		assertTrue(cube.has(Integer.MAX_VALUE, 200, 300));
		assertTrue(cube.has(Integer.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE));

		cube.clear(100, 100, 100);
		assertFalse(cube.has(1, 1, 1));
		assertFalse(cube.has(1, 1, 2));
		assertFalse(cube.has(1, 2, 3));
		assertFalse(cube.has(10, 10, 10));
		assertFalse(cube.has(10, 10, 20));
		assertFalse(cube.has(10, 20, 30));
		assertFalse(cube.has(100, 100, 100));

		cube.clear(100, 100, 200);
		cube.clear(100, 200, 300);
		cube.clear(Integer.MAX_VALUE, 1, 1);
		cube.clear(Integer.MAX_VALUE, 1, 2);
		cube.clear(Integer.MAX_VALUE, 2, 3);
		cube.clear(Integer.MAX_VALUE, 10, 10);
		cube.clear(Integer.MAX_VALUE, 10, 20);
		cube.clear(Integer.MAX_VALUE, 20, 30);
		cube.clear(Integer.MAX_VALUE, 100, 100);
		cube.clear(Integer.MAX_VALUE, 200, 200);
		cube.clear(Integer.MAX_VALUE, 200, 300);
		cube.clear(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(cube.isEmpty());
	}

	@Test
	public void testFind() {
		List<Idx> lst;
		cube = new BitCube();

		cube.set(1, 1, 1);
		cube.set(1, 1, 2);
		cube.set(1, 2, 3);
		cube.set(10, 10, 10);
		cube.set(10, 10, 20);
		cube.set(10, 20, 30);
		cube.set(100, 100, 100);
		cube.set(100, 50, 100);
		cube.set(100, 100, 200);
		cube.set(100, 200, 300);
		cube.set(Integer.MAX_VALUE, 1, 1);
		cube.set(Integer.MAX_VALUE, 1, 2);
		cube.set(Integer.MAX_VALUE, 2, 3);
		cube.set(Integer.MAX_VALUE, 10, 10);
		cube.set(Integer.MAX_VALUE, 10, 20);
		cube.set(Integer.MAX_VALUE, 20, 30);
		cube.set(Integer.MAX_VALUE, 100, 100);
		cube.set(Integer.MAX_VALUE, 200, 200);
		cube.set(Integer.MAX_VALUE, 200, 300);
		cube.set(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

		Iterator<Idx> iter = cube.find(1, 1, 1);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(1, 1, 1), iter.next());
		assertFalse(iter.hasNext());

		iter = cube.find(1, 1, BitConstants.WILD);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(1, 1, 1), iter.next());
		assertEquals(new Idx(1, 1, 2), iter.next());
		assertFalse(iter.hasNext());

		iter = cube.find(100, BitConstants.WILD, 100);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(100, 50, 100), iter.next());
		assertEquals(new Idx(100, 100, 100), iter.next());
		assertFalse(iter.hasNext());

		iter = cube.find(5, 5, 5);
		assertFalse(iter.hasNext());

		lst = cube
				.find(BitConstants.WILD, BitConstants.WILD, BitConstants.WILD)
				.toList();
		assertEquals(20, lst.size());

	}
}
