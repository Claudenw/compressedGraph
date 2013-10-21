package org.compressedGraph;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.xenei.compressedgraph.SparseArray;

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
