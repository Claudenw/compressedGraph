package org.xenei.compressedgraph.bloom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;

import org.xenei.compressedgraph.SerializableTriple;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class BloomGraph extends GraphBase {

	private final BloomCapabilities io;
	private final static SerializableTripleMap TRIPLE_MAP = new SerializableTripleMap();

	public BloomGraph(BloomCapabilities io) {
		if (!io.supportsBloomQuery() && !io.supportsExact()) {
			throw new IllegalArgumentException(
					"BloomCapabilities must support exact searching if bloom query is not supported");
		}
		this.io = io;
	}

	@Override
	protected final ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {

		final SerializableTriple target;
		try {
			target = new SerializableTriple(m.asTriple());
		} catch (IOException e) {
			return NiceIterator.emptyIterator();
		}

		boolean exact = !target.containsWild();
		ExtendedIterator<SerializableTriple> stIter = null;
		if (io.supportsBloomQuery()) {
			stIter = io.find(getBloomValue(target), exact).mapWith(TRIPLE_MAP);

		} else {
			ByteBuffer limit = io.getMaxBloomValue();
			Iterator<Iterator<ByteBuffer>> iter = BloomIndex.getIndexIterator(
					getBloomValue(target), limit).mapWith(
					new Map1<ByteBuffer, Iterator<ByteBuffer>>() {

						@Override
						public Iterator<ByteBuffer> map1(ByteBuffer o) {
							return io.find(o, true);
						}
					});
			stIter = WrappedIterator.createIteratorIterator(iter).mapWith(
					TRIPLE_MAP);
		}

		return stIter.filterKeep(new BloomFilter(target, io)).mapWith(
				new Map1<SerializableTriple, Triple>() {

					@Override
					public Triple map1(SerializableTriple o) {
						try {
							return o.getTriple();
						} catch (IOException e) {
							throw new RuntimeException(
									"Triple not created during iteration");
						}
					}
				});

	}

	private ByteBuffer getBloomValue(SerializableTriple t) {
		byte[] bloom = new byte[4];
		return ByteBuffer.wrap(bloom).order(ByteOrder.BIG_ENDIAN)
				.putInt(0, t.hashCode());
	}

	public static Long getBloomValue(byte[] bloom) {
		byte[] b = new byte[8];
		ByteBuffer buff = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
		buff.position(4);
		buff.put(bloom);
		return Long.valueOf(buff.getLong(0));
	}

	public static ByteBuffer encodeBloomValue(long l) {
		byte[] b = new byte[8];
		ByteBuffer buff = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
		buff.putLong(0, l);
		buff.position(4);
		byte[] dest = new byte[4];
		buff.get(dest);
		return ByteBuffer.wrap(dest);
	}

	@Override
	public final void performAdd(Triple t) {
		SerializableTriple st;

		try {
			st = new SerializableTriple(t);
			boolean dup = false;
			if (io.addsDuplicates()) {
				dup = io.find(getBloomValue(st), true)
						.filterKeep(new ExactMatchFilter(st)).hasNext();
			}
			if (!dup) {
				io.write(getBloomValue(st), st.getByteBuffer());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void performDelete(Triple t) {
		SerializableTriple target;
		try {
			target = new SerializableTriple(t);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		io.delete(getBloomValue(target), target.getByteBuffer());
	}

	@Override
	public void close() {
		io.close();
		super.close();
	}

	@Override
	protected int graphBaseSize() {
		return io.getSize();
	}

	@Override
	public Capabilities getCapabilities() {
		if (capabilities == null) {
			capabilities = new MyCapabilities(io);
		}
		return capabilities;
	}

	public static class ExactMatchFilter extends Filter<ByteBuffer> {
		private ByteBuffer desired;
		private ByteBufferComparator comp;

		ExactMatchFilter(SerializableTriple t) {
			desired = t.getByteBuffer();
			comp = new ByteBufferComparator();
		}

		ExactMatchFilter(ByteBuffer t) {
			desired = t;
			comp = new ByteBufferComparator();
		}

		@Override
		public boolean accept(ByteBuffer o) {
			return comp.compare(desired, o) == 0;
		}
	}

	// public static class ExactKeyMatchFilter extends
	// Filter<SerializableTriple> {
	// int desired;
	//
	// ExactKeyMatchFilter(SerializableTriple t) {
	// desired = t.hashCode();
	// }
	//
	// @Override
	// public boolean accept(SerializableTriple o) {
	// return desired == o.hashCode();
	// }
	//
	// }

	public static class ByteBufferComparator implements Comparator<ByteBuffer> {

		@Override
		public int compare(ByteBuffer arg0, ByteBuffer arg1) {
			if (arg0.capacity() == arg1.capacity()) {
				int len = arg0.capacity();
				arg0.position(0);
				arg1.position(0);
				int retval;
				for (int i = 0; i < len; i++) {
					byte b0 = arg0.get();
					byte b1 = arg1.get();
					retval = compareNibble(BloomIndex.leftNibble(b0),
							BloomIndex.leftNibble(b1));
					if (retval == 0) {
						retval = compareNibble(BloomIndex.rightNibble(b0),
								BloomIndex.rightNibble(b1));
					}
					if (retval != 0) {
						return retval;
					}
				}
				return 0;
			}
			return arg0.capacity() < arg1.capacity() ? -1 : 1;
		}

		private int compareNibble(byte b0, byte b1) {
			return (b0 < b1) ? -1 : (b0 == b1) ? 0 : 1;
		}
	};

	private static class SerializableTripleMap implements
			Map1<ByteBuffer, SerializableTriple> {
		@Override
		public SerializableTriple map1(ByteBuffer o) {
			return new SerializableTriple(o);
		}
	}

	private class MyCapabilities implements Capabilities {

		private BloomCapabilities io;

		MyCapabilities(BloomCapabilities io) {
			this.io = io;
		}

		@Override
		public boolean sizeAccurate() {
			return io.sizeAccurate();
		}

		@Override
		public boolean addAllowed() {
			return io.addAllowed();
		}

		@Override
		public boolean addAllowed(boolean every) {
			return addAllowed();
		}

		@Override
		public boolean deleteAllowed() {
			return io.deleteAllowed();
		}

		@Override
		public boolean deleteAllowed(boolean every) {
			return deleteAllowed();
		}

		@Override
		public boolean canBeEmpty() {
			return io.canBeEmpty();
		}

		@Override
		public boolean iteratorRemoveAllowed() {
			return false;
		}

		@Override
		public boolean findContractSafe() {
			return true;
		}

		@Override
		public boolean handlesLiteralTyping() {
			return false;
		}
	}
}
