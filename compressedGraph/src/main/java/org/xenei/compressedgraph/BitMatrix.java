package org.xenei.compressedgraph;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.jena.atlas.iterator.SingletonIterator;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class BitMatrix implements BitConstants {
	
	private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();
	private SparseArray<SparseBitArray> rows;

	public BitMatrix() {
		rows = new SparseArray<SparseBitArray>();
	}

	public void set(int x, int y) {
		if (x<0 || y<0)
		{
			throw new IllegalArgumentException( String.format("Indexes (%s,%s) must be greater than or equals to 0", x, y ));
		}
		
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			SparseBitArray bs = rows.get(y);
			if (bs == null) {
				bs = new SparseBitArray();
			}
			bs.set(x);
			rows.put(y, bs);
		}
		finally {
			wl.unlock();
		}
	}

	public void clear(int x, int y) {
		if (x<0 || y<0)
		{
			throw new IllegalArgumentException( String.format("Indexes (%s,%s) must be greater than or equals to 0", x, y ));
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			SparseBitArray bs = rows.get(y);
			if (bs != null) {
				bs.clear(x);
				if (bs.isEmpty()) {
					bs = null;
				}
			}
			rows.put(y, bs);
		}
		finally {
			wl.unlock();
		}
	}

	public boolean has(int x, int y) {
		if (x<0 || y<0)
		{
			throw new IllegalArgumentException( String.format("Indexes (%s,%s) must be greater than or equals to 0", x, y ));
		}
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			SparseBitArray bs = rows.get(y);
			return bs == null ? false : bs.get(x);
		}
		finally {
			rl.unlock();
		}
	}

	public boolean isEmpty() {
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			return rows.isEmpty();
		}
		finally {
			rl.unlock();
		}
	}

	public ExtendedIterator<Idx> find(final int x, final int y) {
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			if (isEmpty()) {
				return NiceIterator.emptyIterator();
			}
			if (y < 0) { // y is wild
				return WrappedIterator
						.createIteratorIterator(new Iterator<Iterator<Idx>>() {
							Iterator<Integer> yItr = rows.indexIterator();
	
							public boolean hasNext() {
								return yItr.hasNext();
							}
	
							public Iterator<Idx> next() {
								int y = yItr.next();
								SparseBitArray bs = rows.get(y);
								if (bs == null) {
									return NiceIterator.emptyIterator();
								}
								return getXIterator( x, y, bs );
							}
	
							public void remove() {
								throw new UnsupportedOperationException();
	
							}
						});
			}
			else  {
				SparseBitArray bs = rows.get(y);
				if (bs == null) {
					return NiceIterator.emptyIterator();
				}
				return getXIterator( x, y, bs );
			} 
		} finally {
			rl.unlock();
		}
	}
	
	private ExtendedIterator<Idx> getXIterator( final int x, final int y, SparseBitArray bs ) {
		if (y < 0)
		{
			throw new IllegalArgumentException( String.format("y (%s) may not be less than 0", y ));
		}
		if (x < 0) // x is wild
		{
			return WrappedIterator.create(bs.iterator()).mapWith(new Mapper(y));
		}
		else
		{
			if (bs.get(x)) {
				return WrappedIterator.create(SingletonIterator.create( new Idx( x, y )));
			}
			else
			{
				return NiceIterator.emptyIterator();
			}	
		}
	}

	private static class Mapper implements Map1<Integer, Idx> {

		int y;

		public Mapper(int y) {
			this.y = y;
		}

		public Idx map1(Integer o) {
			return new Idx(o, y);
		}

	}

	public static class Idx implements Comparable<Idx> {
		private int x;
		private int y;

		public Idx(int x, int y) {
			this.x = x;
			this.y = y;
		}

		public int getX() {
			return x;
		}

		public int getY() {
			return y;
		}

		public int compareTo(Idx that) {
			if (this.x < that.x) {
				return -1;
			}
			if (this.x > that.x) {
				return 1;
			}
			// x == x
			if (this.y < that.y) {
				return -1;
			}
			if (this.y > that.y) {
				return 1;
			}

			return 0;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof BitMatrix.Idx) {
				return compareTo((Idx) o) == 0;
			}
			return false;
		}

		@Override
		public int hashCode() {
			long l = x + y;
			return (int) l & 0xFFFFFFFF;
		}
		
		@Override
		public String toString()
		{
			return String.format( "BitMatrix.Idx[%s,%s]", x, y );
		}
	}
}
