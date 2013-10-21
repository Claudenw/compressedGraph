package org.xenei.compressedgraph;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BitSetIterator implements Iterator<Integer>
{
	private BitSet bs;
	private int next;
	
	/*
	 * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
 // operate on index i here
}
	 */
	public BitSetIterator(BitSet bs)
	{
		this.bs = bs;
		next = bs.nextSetBit(0);
	}
	
	public boolean hasNext() {
		return next != -1;
	}

	protected int getNext()
	{
		if (!hasNext())
		{
			throw new NoSuchElementException();
		}
		int retval = next;
		next = bs.nextSetBit(next+1);
		return retval;
	}
	
	public Integer next() {
		return getNext();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}
