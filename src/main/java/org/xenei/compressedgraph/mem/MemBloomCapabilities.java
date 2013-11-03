package org.xenei.compressedgraph.mem;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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

	private NavigableMap<Long, Set<ByteBuffer>> dataMap;
	private Comparator<ByteBuffer> comparator;

	private long count = 0;

	public MemBloomCapabilities() {
		dataMap = new ConcurrentSkipListMap<Long, Set<ByteBuffer>>();
		comparator = new BloomGraph.ByteBufferComparator();
	}

	@Override
	public boolean supportsFullBloom() {
		return true;
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
	public void write(byte[] bloomValue, ByteBuffer data) {
		Long bloom = BloomGraph.getBloomValue(bloomValue);

		Set<ByteBuffer> dataSet = dataMap.get(bloom);
		if (dataSet == null) {
			dataSet = new ConcurrentSkipListSet<ByteBuffer>( comparator );
			dataSet.add(data);
			dataMap.put(bloom, dataSet);
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
	public void delete(byte[] bloomValue, ByteBuffer data) {

		Long bloom = BloomGraph.getBloomValue(bloomValue);
		Set<ByteBuffer> dataSet = dataMap.get(bloom);

		if (dataSet != null) {
			if (dataSet.contains(data)) {
				dataSet.remove(data);
				count--;
			}
		}
	}

	@Override
	public ExtendedIterator<ByteBuffer> find(byte[] bloomValue, boolean exact) {
		Long bloom = BloomGraph.getBloomValue(bloomValue);
		Set<ByteBuffer> set;
		if (exact) {
			set = dataMap.get(bloom);
			if (set == null) {
				return NiceIterator.emptyIterator();
			} else {
				return WrappedIterator.create(set.iterator());
			}

		} else {
			return WrappedIterator
					.createIteratorIterator(WrappedIterator
							.create(dataMap.tailMap(bloom).entrySet()
									.iterator())
							.filterKeep(new MyFilter(bloom))
							.mapWith(
									new Map1<Entry<Long, Set<ByteBuffer>>, Iterator<ByteBuffer>>() {

										@Override
										public Iterator<ByteBuffer> map1(
												Entry<Long, Set<ByteBuffer>> o) {
											return o.getValue().iterator();
										}
									}));
		}

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	private class MyFilter extends Filter<Entry<Long, Set<ByteBuffer>>> {

		Long bloom;

		MyFilter(Long bloom) {
			this.bloom = bloom;
		}

		@Override
		public boolean accept(Entry<Long, Set<ByteBuffer>> o) {
			return (o.getKey() & bloom) == bloom;
		}

	}
}
