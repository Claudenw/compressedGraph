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
import org.xenei.compressedgraph.core.SparseArray;

public class SparseArrayTest {
	
	private SparseArray<String> ary;
	
	@Before
	public void beforeSparseArrayTest()
	{
		ary = new SparseArray<String>();

	}
	
	@Test
	public void testPutGet()
	{
		ary.put(1, "one");
		ary.put(10,  "ten");
		ary.put( 1000,  "one-thousand");
		ary.put( 1000000, "one-million");
		ary.put( Integer.MAX_VALUE, "Max int");
		
		assertEquals( "one", ary.get(1) );
		assertEquals( "ten", ary.get(10) );
		assertEquals( "one-thousand", ary.get(1000) );
		assertEquals( "one-million", ary.get(1000000) );
		assertEquals( "Max int", ary.get(Integer.MAX_VALUE) );
		assertNull( ary.get( 5 ));
	}

	
	@Test
	public void testPutBeforeFirstGet()
	{
		ary.put( Integer.MAX_VALUE, "Max int");
		ary.put(1, "one");
		ary.put(10,  "ten");
		ary.put( 1000,  "one-thousand");
		ary.put( 1000000, "one-million");

		
		assertEquals( "one", ary.get(1) );
		assertEquals( "ten", ary.get(10) );
		assertEquals( "one-thousand", ary.get(1000) );
		assertEquals( "one-million", ary.get(1000000) );
		assertEquals( "Max int", ary.get(Integer.MAX_VALUE) );
		assertNull( ary.get( 5 ));
	}

	
	@Test
	public void testIndexIterator()
	{
		ary.put( Integer.MAX_VALUE, "Max int");
		ary.put(1, "one");
		ary.put(10,  "ten");
		ary.put( 1000,  "one-thousand");
		ary.put( 1000000, "one-million");

		Iterator<Integer> iter = ary.indexIterator();
		
		assertEquals( Integer.valueOf(1), iter.next() );
		assertEquals( Integer.valueOf(10), iter.next() );
		assertEquals( Integer.valueOf(1000), iter.next() );
		assertEquals( Integer.valueOf(1000000), iter.next() );
		assertEquals( Integer.valueOf(Integer.MAX_VALUE), iter.next() );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}
	}
	
	@Test
	public void testIterator()
	{
		ary.put( Integer.MAX_VALUE, "Max int");
		ary.put(1, "one");
		ary.put(10,  "ten");
		ary.put( 1000,  "one-thousand");
		ary.put( 1000000, "one-million");

		Iterator<String> iter = ary.iterator();
		
		assertEquals( "one", iter.next() );
		assertEquals( "ten", iter.next()  );
		assertEquals( "one-thousand", iter.next()  );
		assertEquals( "one-million", iter.next()  );
		assertEquals( "Max int", iter.next()  );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}
	}
	
	@Test
	public void testRemove()
	{
		ary.put( Integer.MAX_VALUE, "Max int");
		ary.put(1, "one");
		ary.put(10,  "ten");
		ary.put( 1000,  "one-thousand");
		ary.put( 1000000, "one-million");
		
		assertFalse( ary.isEmpty() );
		ary.remove(1000);

		Iterator<String> iter = ary.iterator();
		
		assertEquals( "one", iter.next() );
		assertEquals( "ten", iter.next()  );
		assertEquals( "one-million", iter.next()  );
		assertEquals( "Max int", iter.next()  );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}
		
		ary.remove(Integer.MAX_VALUE);

		iter = ary.iterator();
		
		assertEquals( "one", iter.next() );
		assertEquals( "ten", iter.next()  );
		assertEquals( "one-million", iter.next()  );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}
		
		ary.remove(1);
		iter = ary.iterator();
		
		assertEquals( "ten", iter.next()  );
		assertEquals( "one-million", iter.next()  );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}

		ary.remove(10);
		iter = ary.iterator();
		
		assertEquals( "one-million", iter.next()  );
		try {
			iter.next();
			fail("Should have thrown NoSuchElementException");
		}
		catch (NoSuchElementException expected)
		{
			// expected
		}
		ary.remove( 1000000 );
		
		assertFalse( ary.iterator().hasNext() );
		assertTrue( ary.isEmpty() );

	}
}
