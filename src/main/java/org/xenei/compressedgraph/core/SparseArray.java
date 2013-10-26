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
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class SparseArray<T> implements Serializable {
	private Page first;
	private Page last;
	private int pageSize;
	private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();

	public SparseArray() {
		this(4096);
	}

	public SparseArray(int pageSize) {
		this.pageSize = pageSize;
		first = null;
		last = null;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public T remove(int idx) {
		return put(idx, null);
	}

	public T put(int idx, T value) {
		Page page = locatePage(idx);
		if (value == null) {
			if (page == null || !page.contains(idx)) {
				return null;
			}
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			if (page == null || !page.contains(idx)) {
				page = createPage(idx, page);
			}

			T retval = page.set(idx, value);
			if (page.isEmpty()) {
				removePage(page);
			}
			return retval;
		} finally {
			wl.unlock();
		}
	}

	public T get(int idx) {
		Page pg = locatePage(idx);
		if (pg != null && pg.contains(idx)) {
			return pg.get(idx);
		}
		return null;
	}

	public boolean has(int idx) {
		Page pg = locatePage(idx);
		return (pg != null && pg.contains(idx));
	}

	public ExtendedIterator<T> iterator() {
		return WrappedIterator.createIteratorIterator(pageIterator().mapWith(
				new Map1<Page, Iterator<T>>() {

					@Override
					public Iterator<T> map1(Page o) {
						return o.iterator();
					}
				}));
	}

	public ExtendedIterator<Integer> indexIterator() {
		return WrappedIterator.createIteratorIterator(pageIterator().mapWith(
				new Map1<Page, Iterator<Integer>>() {

					@Override
					public Iterator<Integer> map1(Page o) {
						return o.indexIterator();
					}
				}));
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
			int offset = idx / pageSize;
			if (first == null) {
				return null;
			}
			Page curr = first;
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
			int offset = idx / pageSize;
			if (prev == null) {
				// inserting at first
				first = new Page(offset, null, first);
				if (last == null) {
					last = first;
				}
				return first;
			}

			return new Page(offset, prev, prev.next);
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

	public class Page implements Serializable {
		private int offset;
		private Object[] data;
		private transient Page next;
		private transient Page prev;
		private transient final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();

		private Page(int offset, Page prev, Page next) {
			this.offset = offset;
			this.prev = prev;
			this.next = next;
			data = new Object[pageSize];
			if (prev != null) {
				prev.next = this;
			}
			if (next != null) {
				next.prev = this;
			}
		}

		public boolean isEmpty() {
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				for (int i = 0; i < pageSize; i++) {
					if (data[i] != null) {
						return false;
					}
				}
				return true;
			} finally {
				rl.unlock();
			}
		}

		public boolean contains(int idx) {
			return offset == idx / pageSize;
		}

		private T set(int idx, T value) {
			WriteLock wl = LOCK_FACTORY.writeLock();
			wl.lock();
			try {
				int i = idx % pageSize;
				T retval = (T) data[i];
				data[i] = value;
				return retval;
			} finally {
				wl.unlock();
			}

		}

		private T get(int idx) {
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				int i = idx % pageSize;
				return (T) data[i];
			} finally {
				rl.unlock();
			}
		}

		public ExtendedIterator<T> iterator() {
			return WrappedIterator.create(Arrays.asList(data).iterator())
					.filterKeep(new Filter<Object>() {
						@Override
						public boolean accept(Object o) {
							return o != null;
						}
					}).mapWith(new Map1<Object, T>() {

						@Override
						public T map1(Object o) {
							return (T) o;
						}
					});
		}

		public ExtendedIterator<Integer> indexIterator() {
			return WrappedIterator.create(new IdxIterator());
		}

		private class IdxIterator implements Iterator<Integer> {
			private int next;
			private boolean used;
			private boolean done;

			IdxIterator() {
				done = false;
				used = false;
				next = -1;
				findNext();
			}

			private void findNext() {
				if (!done) {
					for (int i = next + 1; i < pageSize; i++) {
						if (data[i] != null) {
							next = i;
							used = false;
							return;
						}
					}
				}
				done = true;
			}

			@Override
			public boolean hasNext() {
				if (used) {
					findNext();
				}
				return !done;
			}

			@Override
			public Integer next() {
				if (used) {
					findNext();
				}
				if (done) {
					throw new NoSuchElementException();
				}
				used = true;
				return next + (offset * pageSize);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		}
	}

}
