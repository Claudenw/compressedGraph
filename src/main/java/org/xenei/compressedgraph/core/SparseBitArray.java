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
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;


import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class SparseBitArray implements BitConstants, Serializable {
	private Page first;
	private Page last;
	private int pageSize;
	private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();

	public SparseBitArray() {
		this(DEFAULT_PAGE_SIZE);
	}

	public SparseBitArray(int pageSize) {
		this.pageSize = pageSize;
		first = null;
		last = null;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public boolean clear(int idx) {
		return set(idx, false);
	}

	public boolean set(int idx) {
		return set(idx, true);
	}

	public boolean set(int idx, boolean value) {
		Page page = locatePage(idx);
		if (!value) {
			if (page == null || !page.contains(idx)) {
				return false;
			}
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			if (page == null || !page.contains(idx)) {
				page = createPage(idx, page);
			}

			boolean retval = page.set(idx, value);
			if (page.isEmpty()) {
				removePage(page);
			}
			return retval;
		} finally {
			wl.unlock();
		}
	}

	public boolean get(int idx) {
		Page pg = locatePage(idx);
		if (pg != null && pg.contains(idx)) {
			return pg.get(idx);
		}
		return false;
	}

	public ExtendedIterator<Integer> iterator() {
		return WrappedIterator.createIteratorIterator(pageIterator().mapWith(
				new Map1<Page, Iterator<Integer>>() {

					@Override
					public Iterator<Integer> map1(Page o) {
						return o.indexIterator();
					}
				}));
	}

	/**
	 * Return the page that the index is on or the parent of the page Will
	 * return null if the parent is before the start.
	 * 
	 * @param idx
	 * @return
	 */
	private Page locatePage(int idx) {
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			int offset = idx / this.pageSize;
			if (first == null) {
				return null;
			}
			Page curr = null;
			if (offset >= last.offset) {
				// may be on last page
				curr = last;
			} else {
				// not on last page
				curr = first;
			}
			while (curr.offset < offset) {
				if (curr.next == null) {
					return curr;
				}
				curr = curr.next;
			}
			if (curr.offset == offset) {
				return curr;
			}
			return curr.prev;
		} finally {
			rl.unlock();
		}
	}

	private Page createPage(int idx, Page prev) {
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			int offset = idx / this.pageSize;
			if (prev == null) {
				// inserting at first
				first = new Page(offset, null, first);
				if (last == null) {
					last = first;
				}
				return first;
			}

			Page retval = new Page(offset, prev, prev.next);
			if (retval.next == null) {
				last = retval;
			}
			return retval;
		} finally {
			wl.unlock();
		}
	}

	private void removePage(Page page) {
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			if (page.prev != null) {
				page.prev.next = page.next;
			}
			if (page.next != null) {
				page.next.prev = page.prev;
			}
			if (first == page) {
				first = page.next;
			}
			if (last == page) {
				last = page.prev;
			}
		} finally {
			wl.unlock();
		}
	}

	private ExtendedIterator<Page> pageIterator() {
		return WrappedIterator.create(new Iterator<Page>() {
			Page current = first;

			@Override
			public boolean hasNext() {
				return current != null;
			}

			@Override
			public Page next() {
				Page retval = current;
				current = current.next;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		});
	}

	private int getPageCount() {
		int i = 0;
		Page p = first;
		while (p != null) {
			i++;
			p = p.next;
		}
		return i;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.write(pageSize);
		Page p = first;
		out.writeInt(getPageCount());

		while (p != null) {
			out.writeObject(p);
			p = p.next;
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		pageSize = in.readInt();
		int pageCount = in.readInt();
		for (int i = 0; i < pageCount; i++) {

			Page p = (Page) in.readObject();
			if (first == null) {
				first = p;
				last = p;
			} else {
				last.next = p;
				p.prev = last;
				last = p;
			}
		}
	}

	private class Page implements Serializable {
		private int offset;
		private BitSet data;
		private transient Page next;
		private transient Page prev;
		private transient final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();

		private Page(int offset, Page prev, Page next) {
			this.offset = offset;
			this.prev = prev;
			this.next = next;
			data = new BitSet(pageSize);
			if (prev != null) {
				prev.next = this;
			}
			if (next != null) {
				next.prev = this;
			}
		}

		public boolean contains(int idx) {
			return offset == idx / pageSize;
		}

		public boolean isEmpty() {
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				return data.isEmpty();
			} finally {
				rl.unlock();
			}
		}

		private synchronized boolean set(int idx, boolean value) {
			WriteLock wl = LOCK_FACTORY.writeLock();
			wl.lock();
			try {
				int pageIdx = idx % pageSize;
				boolean retval = data.get(pageIdx);
				if (value) {
					data.set(pageIdx);
				} else {
					data.clear(pageIdx);
				}
				return retval;
			} finally {
				wl.unlock();
			}
		}

		private boolean get(int idx) {
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				int pageIdx = idx % pageSize;
				return data.get(pageIdx);
			} finally {
				rl.unlock();
			}
		}

		public ExtendedIterator<Integer> indexIterator() {
			return WrappedIterator.create(new PageIterator());
		}

		private class PageIterator extends BitSetIterator {
			PageIterator() {
				super(data);
			}

			@Override
			public Integer next() {
				return getNext() + (offset * pageSize);
			}
		}
	}

}
