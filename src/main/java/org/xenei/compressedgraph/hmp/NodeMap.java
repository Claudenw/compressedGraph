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
package org.xenei.compressedgraph.hmp;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.xenei.compressedgraph.CompressedNode;
import org.xenei.compressedgraph.INodeMap;
import com.hp.hpl.jena.graph.Node;

/**
 * A node map that stores data in 2 random access files.
 * 
 */
public class NodeMap implements INodeMap {

	private RandomAccessFile data;
	private RandomAccessFile idx;
	private Map<Integer, ListSet<Long>> map;
	private int size;
	final static int MAX_ENTRIES = 100;
	private Map<Integer, CompressedNode> cache;

	public NodeMap() throws IOException {
		File f = File.createTempFile("hmp", ".dat");

		data = new RandomAccessFile(f, "rw");
		String fn = f.getAbsolutePath();
		fn = fn.substring(0, fn.lastIndexOf('.')) + ".idx";
		idx = new RandomAccessFile(fn, "rw");
		map = new HashMap<Integer, ListSet<Long>>();
		size = 0;
		cache = new LinkedHashMap<Integer, CompressedNode>(MAX_ENTRIES + 1,
				.75F, true) {
			// This method is called just after a new entry has been added
			@Override
			public boolean removeEldestEntry(Map.Entry eldest) {
				return size() > MAX_ENTRIES;
			}
		};
	}

	private CompressedNode updateCache(CompressedNode cn) {
		cache.put(cn.getIdx(), cn);
		return cn;
	}

	@Override
	public CompressedNode get(Node n) throws IOException {
		if (n == null || n == Node.ANY) {
			return CompressedNode.ANY;
		}

		for (CompressedNode cn : cache.values()) {
			if (n.equals(cn.getNode())) {
				return updateCache(cn);
			}
		}

		CompressedNode cn;
		ListSet<Long> candidates = map.get(n.hashCode());

		if (candidates != null) {
			for (Long i : candidates) {
				cn = read(i);
				if (n.equals(cn.getNode())) {
					return updateCache(cn);
				}
			}
		}

		cn = new CompressedNode(n, size);
		size++;
		if (candidates == null) {
			candidates = new ListSet<Long>();
			map.put(cn.hashCode(), candidates);
		}

		candidates.add(write(cn));
		return updateCache(cn);
	}

	@Override
	public CompressedNode get(int idx) throws IOException {
		return read(idx);
	}

	@Override
	public void close() {
	}

	@Override
	public int count() {
		return size;
	}

	private long write(CompressedNode cn) throws IOException {
		long retval = data.length();
		byte[] buffer = cn.getBuffer();
		data.seek(retval);
		data.writeInt(buffer.length);
		data.write(buffer);
		idx.seek(cn.getIdx() * 8);
		idx.writeLong(retval);
		return retval;
	}

	// read an index
	private CompressedNode read(int idxNum) throws IOException {
		CompressedNode cn = cache.get(idxNum);
		if (cn == null) {
			idx.seek(idxNum * 8);
			long pos = idx.readLong();
			cn = read(pos);
		}
		return updateCache(cn);
	}

	// rad a position
	private CompressedNode read(long pos) throws IOException {
		data.seek(pos);
		int length = data.readInt();
		byte[] value = new byte[length];
		data.readFully(value);
		return new CompressedNode(value);
	}

	public static class ListSet<T> extends ArrayList<T> {
		public ListSet() {
			super();
		}

		@Override
		public boolean add(T e) {
			if (!this.contains(e)) {
				return super.add(e);
			}
			return false;
		}
	}

}
