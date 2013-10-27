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

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over a bitset that provides the integer position for each bit
 * that is set.
 * 
 */
public class BitSetIterator implements Iterator<Integer> {
	private BitSet bs;
	private int next;

	/*
	 * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) { //
	 * operate on index i here }
	 */
	public BitSetIterator(BitSet bs) {
		this.bs = bs;
		next = bs.nextSetBit(0);
	}

	@Override
	public boolean hasNext() {
		return next != -1;
	}

	protected int getNext() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		int retval = next;
		next = bs.nextSetBit(next + 1);
		return retval;
	}

	@Override
	public Integer next() {
		return getNext();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
