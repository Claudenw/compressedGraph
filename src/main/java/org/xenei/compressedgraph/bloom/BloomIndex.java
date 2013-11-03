package org.xenei.compressedgraph.bloom;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.xenei.compressedgraph.bloom.BloomGraph.ByteBufferComparator;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class BloomIndex {

	private static final int NIBBLE_SIZE = 4;
	private final static byte[][] MASK = {
	 {0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xA,0xB,0xC,0xD,0xE,0xF}, //0x0
	 {0x1,0x3,0x5,0x7,0x9,0xB,0xD,0xF}, // 0x1
	 {0x2,0x3,0x6,0x7,0xA,0xB,0xE,0xF}, // 0x2
	 {0x3,0x7,0xb,0xf}, 				// 0x3
	 {0x4,0x5,0x5,0x7,0xc,0xd,0xe,0xf}, // 0x4
	 {0x5,0x7,0xd,0xf}, //0x5
	 {0x6,0x7,0xe,0xf}, //0x6
	 {0x7,0xf}, //0x7
	 {0x8,0x9,0xA,0xB,0xC,0xD,0xE,0xF}, //0x8
	 {0x9,0xb,0xd,0xf}, //0x9
	 {0xA,0xb,0xe,0xf}, //0xA
	 {0xB,0xf}, // 0xB
	 {0xC,0xd,0xe,0xf}, // 0xC
	 {0xD,0xF}, // 0xD
	 {0xE,0xF}, // 0xE
	 {0xF}, // 0xF
	};

	public static byte leftNibble(byte pattern)
	{
		return (byte) ((pattern & 0xF0) >>> NIBBLE_SIZE);
	}
	
	public static byte rightNibble(byte pattern)
	{
		return (byte) (pattern & 0x0F);
	}
	
	private static int getExpansionCount( byte left, byte right)
	{
		return MASK[left].length * MASK[right].length;	
	}
	
	private static int getExpansionCount( byte pattern )
	{
		return getExpansionCount( leftNibble(pattern), rightNibble(pattern));
	}
	
	private static byte getExpansionByte( byte pattern, int idx)
	{
		byte left = leftNibble(pattern);
		byte right = rightNibble(pattern);
		int leftIdx = idx / MASK[right].length;
		int rightIdx = idx % MASK[right].length;
		return (byte) ( MASK[left][leftIdx] << NIBBLE_SIZE | MASK[right][rightIdx]);
	}
	
	public static ExtendedIterator<ByteBuffer> getIndexIterator( ByteBuffer pattern )
	{
		byte[] buff = new byte[pattern.capacity()];
		Arrays.fill(buff,  (byte)0xFF);
		return WrappedIterator.create( new BufferIterator( pattern, ByteBuffer.wrap( buff ) ));
	}
	
	public static ExtendedIterator<ByteBuffer> getIndexIterator( ByteBuffer pattern, ByteBuffer limit )
	{
		return WrappedIterator.create( new BufferIterator( pattern, limit ));
	}
	
	public static class BufferIterator implements Iterator<ByteBuffer> 
	{
		private boolean done;
		private boolean used;
		private final ByteBuffer limit;
		private final ByteBuffer pattern;
		private ByteBuffer next;
		private int[] maxPos;
		private int[] currentPos;
		private ByteBufferComparator comp = new ByteBufferComparator();
		
		public BufferIterator( ByteBuffer pattern, ByteBuffer limit )
		{
			done = false;
			if (pattern.capacity() == 0)
			{
				done = true;
			}
			{
				used = true; // make if find the next value
				this.limit = limit;
				this.next = null;
				maxPos = new int[pattern.capacity()];
				currentPos = new int[pattern.capacity()];
				for (int i=0;i<pattern.capacity();i++)
				{
					currentPos[i] = 0;
					maxPos[i] = getExpansionCount( pattern.get(i));
				}
				currentPos[0] = -1;
				this.pattern = pattern;
			}
		}
		
		@Override
		public boolean hasNext() {
			if (done)
			{
				return false;
			}
			if (used)
			{
				for (int i=0;i<pattern.capacity();i++)
				{
					if (currentPos[i]+1 < maxPos[i])
					{
						increment();
						next = ByteBuffer.allocate( pattern.capacity() );
						for (int j=0;j<pattern.capacity();j++)
						{
							next.put( j, getExpansionByte( pattern.get(j), currentPos[j]) );
						}
						used = false;
						done = comp.compare( next, limit) > 0;
						return !done;
					}
				}
				done = true;
				return false;
			}
			return true;
			
		}
		
		private void increment()
		{
			int i = 0;
			currentPos[i]++;
			while (currentPos[i] == maxPos[i])
			{
				currentPos[i++] = 0;
				if (i>pattern.capacity())
				{
					throw new NoSuchElementException();
				}
				currentPos[i]++;
			}
		}
		
		@Override
		public ByteBuffer next() {
			if (!hasNext())
			{
				 throw new NoSuchElementException();
			}
			used = true;
			return next;	
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		
	}

}
