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

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;


import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class BitCube implements BitConstants, Serializable {

	private SparseArray<BitMatrix> depth;
	private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();
	private final int pageSize;
	private int size;

	public BitCube() {
		this(DEFAULT_PAGE_SIZE);
	}

	public BitCube(int pageSize) {
		this.pageSize = pageSize;
		depth = new SparseArray<BitMatrix>(pageSize);
		size = 0;
	}

	public void set(int x, int y, int z) {
		if (x < 0 || y < 0 || x < 0) {
			throw new IllegalArgumentException(String.format(
					"Indexes (%s,%s,%s) must be greater than or equals to 0",
					x, y, z));
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			BitMatrix bm = depth.get(z);
			if (bm == null) {
				bm = new BitMatrix(pageSize);
			}
			bm.set(x, y);
			depth.put(z, bm);
			size++;
		} finally {
			wl.unlock();
		}
	}

	public void clear(int x, int y, int z) {
		if (x < 0 || y < 0 || x < 0) {
			throw new IllegalArgumentException(String.format(
					"Indexes (%s,%s,%s) must be greater than or equals to 0",
					x, y, z));
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			BitMatrix bm = depth.get(z);
			if (bm != null) {
				bm.clear(x, y);
				if (bm.isEmpty()) {
					bm = null;
				}
			}
			depth.put(z, bm);
			size--;
		} finally {
			wl.unlock();
		}
	}

	public boolean has(int x, int y, int z) {
		if (x < 0 || y < 0 || x < 0) {
			throw new IllegalArgumentException(String.format(
					"Indexes (%s,%s,%s) must be greater than or equals to 0",
					x, y, z));
		}
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			BitMatrix bm = depth.get(z);
			return bm == null ? false : bm.has(x, y);
		} finally {
			rl.unlock();
		}
	}

	public boolean isEmpty() {
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			return depth.isEmpty();
		} finally {
			rl.unlock();
		}
	}

	public int getSize() {
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			return size;
		} finally {
			rl.unlock();
		}
	}

	public ExtendedIterator<Idx> find(final int x, final int y, final int z) {
		if (isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		if (z < 0) { // z is wild
			return WrappedIterator
					.createIteratorIterator(new Iterator<Iterator<Idx>>() {
						Iterator<Integer> zItr = depth.indexIterator();

						@Override
						public boolean hasNext() {
							return zItr.hasNext();
						}

						@Override
						public Iterator<Idx> next() {
							int z = zItr.next();
							return WrappedIterator.create(
									depth.get(z).find(x, y)).mapWith(
									new Mapper(z));
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException();

						}
					});
		} else { // z is fixed
			BitMatrix bm = depth.get(z);
			if (bm == null) {
				return NiceIterator.emptyIterator();
			}
			return depth.get(z).find(x, y).mapWith(new Mapper(z));
		}
	}

	// private void writeObject(java.io.ObjectOutputStream out)
	// throws IOException {
	// out.writeInt( pageSize );
	// out.writeInt( size );
	// out.writeObject( depth );
	// }
	// private void readObject(java.io.ObjectInputStream in)
	// throws IOException, ClassNotFoundException
	// {
	// pageSize = in.readInt();
	// size = in.readInt();
	//
	// }
	//
	public static class Idx implements Comparable<Idx> {
		private int x;
		private int y;
		private int z;

		public Idx(int x, int y, int z) {
			this.x = x;
			this.y = y;
			this.z = z;
		}

		Idx(BitMatrix.Idx mIdx, int z) {
			this(mIdx.getX(), mIdx.getY(), z);
		}

		public int getX() {
			return x;
		}

		public int getY() {
			return y;
		}

		public int getZ() {
			return z;
		}

		@Override
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
			// y == y
			if (this.z < that.z) {
				return -1;
			}
			if (this.z > that.z) {
				return 1;
			}
			return 0;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof BitCube.Idx) {
				return compareTo((Idx) o) == 0;
			}
			return false;
		}

		@Override
		public int hashCode() {
			long l = x + y + z;
			return (int) l & 0xFFFFFFFF;
		}

		@Override
		public String toString() {
			return String.format("BitCube.Idx[%s,%s,%s]", x, y, z);
		}
	}

	private static class Mapper implements Map1<BitMatrix.Idx, Idx> {
		int z;

		public Mapper(int z) {
			this.z = z;
		}

		@Override
		public Idx map1(BitMatrix.Idx o) {
			return new Idx(o, z);
		}

	}
}
