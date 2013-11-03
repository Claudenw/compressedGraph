package org.xenei.compressedgraph.mem;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.xenei.compressedgraph.bloom.BloomCapabilities;
import org.xenei.compressedgraph.bloom.BloomGraph;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class MemBloomCapabilities implements BloomCapabilities {

	private NavigableMap<ByteBuffer, Set<ByteBuffer>> dataMap;
	private Comparator<ByteBuffer> comparator;

	private long count = 0;

	public MemBloomCapabilities() {
		comparator = new BloomGraph.ByteBufferComparator();
		dataMap = new ConcurrentSkipListMap<ByteBuffer, Set<ByteBuffer>>( comparator );
	}

	@Override
	public boolean supportsBloomQuery() {
		return false;
	}

	@Override
	public boolean supportsExact() {
		return true;
	}

	@Override
	public boolean canBeEmpty() {
		return true;
	}

	@Override
	public boolean sizeAccurate() {
		return false;
	}

	@Override
	public int getSize() {
		return (int) ((count >= (long)Integer.MAX_VALUE)? Integer.MAX_VALUE : count);
	}

	@Override
	public boolean addAllowed() {
		return true;
	}

	@Override
	public boolean addsDuplicates() {
		return false;
	}
	
	@Override
	public void write(ByteBuffer bloomValue, ByteBuffer data) {
		Set<ByteBuffer> dataSet = dataMap.get(bloomValue);
		if (dataSet == null) {
			dataSet = new ConcurrentSkipListSet<ByteBuffer>( comparator );
			dataSet.add(data);
			dataMap.put(bloomValue, dataSet);
			count++;
		} else {
			
			if (!dataSet.contains(data)) {
				dataSet.add(data);
				count++;
			}
		}
	}

	@Override
	public boolean deleteAllowed() {
		return true;
	}

	@Override
	public void delete(ByteBuffer bloomValue, ByteBuffer data) {
		Set<ByteBuffer> dataSet = dataMap.get(bloomValue);
		if (dataSet != null) {
			if (dataSet.contains(data)) {
				dataSet.remove(data);
				count--;
			}
		}
	}

	@Override
	public ExtendedIterator<ByteBuffer> find(ByteBuffer bloomValue, boolean exact) {
		Set<ByteBuffer> set;
		if (exact) {
			set = dataMap.get(bloomValue);
			if (set == null) {
				return NiceIterator.emptyIterator();
			} else {
				return WrappedIterator.create(set.iterator());
			}

		} else {
			throw new UnsupportedOperationException( "Bloom query is not supported");
		}

	}


	@Override
	public ByteBuffer getMaxBloomValue() {
		return dataMap.lastEntry().getKey();
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
}
