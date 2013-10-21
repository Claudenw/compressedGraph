package org.xenei.compressedgraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class SparseArray<T> {
	private Page first;
	private Page last;
	private static final int PAGE_SIZE=64;
	private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();
	
	public SparseArray()
	{
		first = null;
		last = null;
	}
	
	public boolean isEmpty()
	{
		return first==null;
	}
	
	public T remove( int idx )
	{
		return put( idx, null );
	}
	
	public T put( int idx, T value )
	{
		Page page = locatePage(idx);
		if (value == null)
		{
			if (page == null || ! page.contains(idx))
			{
				return null;
			}
		}
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			if (page == null || ! page.contains(idx))
			{
				page = createPage( idx, page );
			}
		
			T retval =  page.set(idx, value );
			if (page.isEmpty())
			{
				removePage( page );
			}
			return retval;
		}
		finally {
			wl.unlock();
		}
	}
	
	public T get( int idx )
	{
		Page pg = locatePage(idx);
		if (pg != null && pg.contains(idx))
		{
			return pg.get(idx);
		}
		return null;	
	}
	
	public boolean has( int idx )
	{
		Page pg = locatePage(idx);
		return (pg !=null && pg.contains(idx));
	}
	
	public ExtendedIterator<T> iterator()
	{
		return WrappedIterator.createIteratorIterator(pageIterator().mapWith(new Map1<Page,Iterator<T>>(){

			public Iterator<T> map1(Page o) {
				return o.iterator();
			}}));
	}
	
	public ExtendedIterator<Integer> indexIterator()
	{
		return WrappedIterator.createIteratorIterator(pageIterator().mapWith(new Map1<Page,Iterator<Integer>>(){

			public Iterator<Integer> map1(Page o) {
				return o.indexIterator();
			}}));
	}
	
	/**
	 * Return the page that the index is on or the parent of the page
	 * Will return null if the parent is before the start.
	 * @param idx
	 * @return
	 */
	private Page locatePage( int idx )
	{
		ReadLock rl = LOCK_FACTORY.readLock();
		rl.lock();
		try {
			int offset= idx/PAGE_SIZE;
			if (first == null)
			{
				return null;			
			}
			Page curr = first;
			while (curr.offset<offset)
			{
				if (curr.next == null)
				{
					return curr;
				}
				curr = curr.next;
			}
			if (curr.offset==offset)
			{
				return curr;
			}
			return curr.prev;
		}
		finally {
			rl.unlock();
		}
	}
	
	private Page createPage( int idx, Page prev  )
	{
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			int offset = idx/PAGE_SIZE;
			if (prev == null)
			{
				// inserting at first
				first = new Page( offset, null, first );
				if (last == null)
				{
					last = first;
				}
				return first;
			}
			
			return new Page( offset, prev, prev.next );
		}
		finally {
			wl.unlock();
		}	
	}
	
	private void removePage( Page page )
	{
		WriteLock wl = LOCK_FACTORY.writeLock();
		wl.lock();
		try {
			if (page.prev != null)
			{
				page.prev.next = page.next;
			}
			if (page.next != null)
			{
				page.next.prev = page.prev;
			}
			if (first == page)
			{
				first = page.next;
			}
			if (last == page)
			{
				last = page.prev;
			}
		} finally {
			wl.unlock();
		}
	}
	
	private ExtendedIterator<Page> pageIterator()
	{
		return WrappedIterator.create(new Iterator<Page>(){
			Page current = first;
			
			public boolean hasNext() {
				return current != null;
			}

			public Page next() {
				Page retval = current;
				current = current.next;
				return retval;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}});
	}
	
	
	private class Page {
		private int offset;
		private Object[] data;
		private Page next;
		private Page prev;
		private final ReentrantReadWriteLock LOCK_FACTORY = new ReentrantReadWriteLock();
		
		private Page( int offset, Page prev, Page next)
		{
			this.offset = offset;
			this.prev = prev;
			this.next = next;
			data = new Object[PAGE_SIZE];
			if (prev != null)
			{
				prev.next=this;
			}
			if (next != null)
			{
				next.prev=this;
			}
		}
		
		public boolean isEmpty()
		{
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				for (int i=0;i<PAGE_SIZE;i++)
				{
					if (data[i] != null)
					{
						return false;
					}
				}
				return true;
			}
			finally {
				rl.unlock();
			}
		}
		
		public boolean contains(int idx)
		{
			return offset == idx/PAGE_SIZE;
		}
		
		private T set(int idx, T value)
		{	
			WriteLock wl = LOCK_FACTORY.writeLock();
			wl.lock();
			try {
				int i = idx % PAGE_SIZE;
				T  retval = (T)data[i];
				data[i] = value;
				return retval;
			} finally {
				wl.unlock();
			}
			
		}
		
		private T get(int idx)
		{
			ReadLock rl = LOCK_FACTORY.readLock();
			rl.lock();
			try {
				int i = idx % PAGE_SIZE;
				return (T)data[i];
			} finally {
				rl.unlock();
			}
		}
		
		public ExtendedIterator<T> iterator ()
		{
			return WrappedIterator.create(Arrays.asList( data ).iterator()).filterKeep( new Filter<Object>(){
				@Override
				public boolean accept(Object o) {
					return o!=null;
				}}).mapWith(new Map1<Object,T>(){

					public T map1(Object o) {
						return (T)o;
					}});	
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
			
			private void findNext() 
			{
				if (!done) {
					for (int i=next+1;i<PAGE_SIZE;i++)
					{
						if (data[i] != null)
						{
							next =i;
							used = false;
							return;
						}
					}
				}
				done = true;
			}
			
			public boolean hasNext() {
				if (used)
				{
					findNext();
				}
				return !done;
			}

			public Integer next() {
				if (used)
				{
					findNext();
				}
				if (done)
				{
					throw new NoSuchElementException();
				}
				used = true;
				return next+(offset*PAGE_SIZE);
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		}
	}

}
