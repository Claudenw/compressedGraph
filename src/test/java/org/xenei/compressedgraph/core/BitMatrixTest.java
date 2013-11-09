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
import org.xenei.compressedgraph.EnumeratedNode;
import org.xenei.compressedgraph.core.BitMatrix;
import org.xenei.compressedgraph.core.BitMatrix.Idx;

public class BitMatrixTest {

	private BitMatrix matrix;

	@Test
	public void testSetHas() {
		matrix = new BitMatrix();

		matrix.set(1, 1);
		matrix.set(1, 2);
		matrix.set(1, 3);
		matrix.set(10, 10);
		matrix.set(10, 20);
		matrix.set(10, 30);
		matrix.set(100, 100);
		matrix.set(100, 200);
		matrix.set(100, 300);
		matrix.set(Integer.MAX_VALUE, 1);
		matrix.set(Integer.MAX_VALUE, 2);
		matrix.set(Integer.MAX_VALUE, 3);
		matrix.set(Integer.MAX_VALUE, 10);
		matrix.set(Integer.MAX_VALUE, 20);
		matrix.set(Integer.MAX_VALUE, 30);
		matrix.set(Integer.MAX_VALUE, 100);
		matrix.set(Integer.MAX_VALUE, 200);
		matrix.set(Integer.MAX_VALUE, 300);
		matrix.set(Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(matrix.has(1, 1));
		assertTrue(matrix.has(1, 2));
		assertTrue(matrix.has(1, 3));
		assertTrue(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		assertFalse(matrix.has(11, 1));
		assertFalse(matrix.has(1, 12));
		assertFalse(matrix.has(11, 3));
		assertFalse(matrix.has(110, 10));
		assertFalse(matrix.has(10, 120));
		assertFalse(matrix.has(110, 30));
		assertFalse(matrix.has(1100, 100));
		assertFalse(matrix.has(100, 1200));
		assertFalse(matrix.has(1100, 300));
		assertFalse(matrix.has(Integer.MAX_VALUE, 11));
		assertFalse(matrix.has(Integer.MAX_VALUE, 12));
		assertFalse(matrix.has(Integer.MAX_VALUE, 13));
		assertFalse(matrix.has(Integer.MAX_VALUE, 110));
		assertFalse(matrix.has(Integer.MAX_VALUE, 120));
		assertFalse(matrix.has(Integer.MAX_VALUE, 130));
		assertFalse(matrix.has(Integer.MAX_VALUE, 1100));
		assertFalse(matrix.has(Integer.MAX_VALUE, 1200));
		assertFalse(matrix.has(Integer.MAX_VALUE, 1300));
		assertFalse(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE - 1));

	}

	@Test
	public void testClear() {
		matrix = new BitMatrix();

		matrix.set(1, 1);
		matrix.set(1, 2);
		matrix.set(1, 3);
		matrix.set(10, 10);
		matrix.set(10, 20);
		matrix.set(10, 30);
		matrix.set(100, 100);
		matrix.set(100, 200);
		matrix.set(100, 300);
		matrix.set(Integer.MAX_VALUE, 1);
		matrix.set(Integer.MAX_VALUE, 2);
		matrix.set(Integer.MAX_VALUE, 3);
		matrix.set(Integer.MAX_VALUE, 10);
		matrix.set(Integer.MAX_VALUE, 20);
		matrix.set(Integer.MAX_VALUE, 30);
		matrix.set(Integer.MAX_VALUE, 100);
		matrix.set(Integer.MAX_VALUE, 200);
		matrix.set(Integer.MAX_VALUE, 300);
		matrix.set(Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(matrix.has(1, 1));
		assertTrue(matrix.has(1, 2));
		assertTrue(matrix.has(1, 3));
		assertTrue(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(1, 1);
		assertFalse(matrix.has(1, 1));
		assertTrue(matrix.has(1, 2));
		assertTrue(matrix.has(1, 3));
		assertTrue(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(1, 2);
		assertFalse(matrix.has(1, 2));
		assertTrue(matrix.has(1, 3));
		assertTrue(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(1, 3);
		assertFalse(matrix.has(1, 3));
		assertTrue(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(10, 10);
		assertFalse(matrix.has(10, 10));
		assertTrue(matrix.has(10, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(10, 20);
		assertFalse(matrix.has(1, 20));
		assertTrue(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(10, 30);
		assertFalse(matrix.has(10, 30));
		assertTrue(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(100, 100);
		assertFalse(matrix.has(100, 100));
		assertTrue(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(100, 200);
		assertFalse(matrix.has(100, 200));
		assertTrue(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(100, 300);
		assertFalse(matrix.has(100, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(Integer.MAX_VALUE, 1);
		assertFalse(matrix.has(Integer.MAX_VALUE, 1));
		assertTrue(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(Integer.MAX_VALUE, 2);
		assertFalse(matrix.has(Integer.MAX_VALUE, 2));
		assertTrue(matrix.has(Integer.MAX_VALUE, 3));
		assertTrue(matrix.has(Integer.MAX_VALUE, 10));
		assertTrue(matrix.has(Integer.MAX_VALUE, 20));
		assertTrue(matrix.has(Integer.MAX_VALUE, 30));
		assertTrue(matrix.has(Integer.MAX_VALUE, 100));
		assertTrue(matrix.has(Integer.MAX_VALUE, 200));
		assertTrue(matrix.has(Integer.MAX_VALUE, 300));
		assertTrue(matrix.has(Integer.MAX_VALUE, Integer.MAX_VALUE));

		matrix.clear(Integer.MAX_VALUE, 3);
		matrix.clear(Integer.MAX_VALUE, 10);
		matrix.clear(Integer.MAX_VALUE, 20);
		matrix.clear(Integer.MAX_VALUE, 30);
		matrix.clear(Integer.MAX_VALUE, 100);
		matrix.clear(Integer.MAX_VALUE, 200);
		matrix.clear(Integer.MAX_VALUE, 300);
		matrix.clear(Integer.MAX_VALUE, Integer.MAX_VALUE);

		assertTrue(matrix.isEmpty());
	}

	@Test
	public void testFind() {
		List<Idx> lst;
		matrix = new BitMatrix();

		matrix.set(1, 1);
		matrix.set(1, 2);
		matrix.set(1, 3);
		matrix.set(10, 10);
		matrix.set(10, 20);
		matrix.set(10, 30);
		matrix.set(100, 100);
		matrix.set(100, 200);
		matrix.set(100, 300);
		matrix.set(Integer.MAX_VALUE, 1);
		matrix.set(Integer.MAX_VALUE, 2);
		matrix.set(Integer.MAX_VALUE, 3);
		matrix.set(Integer.MAX_VALUE, 10);
		matrix.set(Integer.MAX_VALUE, 20);
		matrix.set(Integer.MAX_VALUE, 30);
		matrix.set(Integer.MAX_VALUE, 100);
		matrix.set(Integer.MAX_VALUE, 200);
		matrix.set(Integer.MAX_VALUE, 300);
		matrix.set(Integer.MAX_VALUE, Integer.MAX_VALUE);

		lst = matrix.find(1, 1).toList();
		Iterator<Idx> iter = matrix.find(1, 1);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(1, 1), iter.next());
		assertFalse(iter.hasNext());

		iter = matrix.find(1, EnumeratedNode.WILD);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(1, 1), iter.next());
		assertEquals(new Idx(1, 2), iter.next());
		assertEquals(new Idx(1, 3), iter.next());
		assertFalse(iter.hasNext());

		iter = matrix.find(EnumeratedNode.WILD, 100);
		assertTrue(iter.hasNext());
		assertEquals(new Idx(100, 100), iter.next());
		assertEquals(new Idx(Integer.MAX_VALUE, 100), iter.next());
		assertFalse(iter.hasNext());

		iter = matrix.find(5, 5);
		assertFalse(iter.hasNext());

		lst = matrix.find(EnumeratedNode.WILD, EnumeratedNode.WILD).toList();
		assertEquals(19, lst.size());

	}
}
