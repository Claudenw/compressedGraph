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
package org.xenei.compressedgraph.mem;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.xenei.compressedgraph.SerializableNode;
import org.xenei.compressedgraph.INodeMap;
import com.hp.hpl.jena.graph.Node;

/**
 * A node map that stores everyting in memory.
 * 
 */
public class NodeMap implements INodeMap, Serializable {
	private List<SerializableNode> lst;
	private Map<SerializableNode, SerializableNode> map;

	public NodeMap() {
		lst = new ArrayList<SerializableNode>();
		map = new HashMap<SerializableNode, SerializableNode>();
	}

	@Override
	public synchronized SerializableNode get(Node n) throws IOException {
		if (n == null || n == Node.ANY) {
			return SerializableNode.ANY;
		}

		SerializableNode wild = new SerializableNode(n);
		SerializableNode cn = map.get(wild);

		if (cn == null) {
			cn = add(wild);
		}
		return cn;
	}

	@Override
	public SerializableNode get(int idx) {
		return lst.get(idx);
	}

	private synchronized SerializableNode add(SerializableNode n) {
		int i = lst.size();
		n.setIdx(i);
		lst.add(n);
		map.put(n, n);
		if (map.size() != lst.size()) {
			throw new IllegalStateException("lists out of order");
		}
		return n;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(lst.size());
		for (int i = 0; i < lst.size(); i++) {
			out.writeObject(lst.get(i));
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			SerializableNode cn = (SerializableNode) in.readObject();
			if (lst.size() <= cn.getIdx()) {
				int fill = cn.getIdx() - lst.size() + 1;
				lst.addAll(Arrays.asList(new SerializableNode[fill]));
			}
			lst.set(cn.getIdx(), cn);
			map.put(cn, cn);
		}
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public int count() {
		return lst.size();
	}
}
