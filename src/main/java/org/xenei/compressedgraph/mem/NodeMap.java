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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.compressedgraph.core.BitConstants;
import org.xenei.compressedgraph.covert.CompressedNode;

import com.hp.hpl.jena.graph.Node;

public class NodeMap implements Serializable {
	private List<CompressedNode> lst;
	private Map<CompressedNode, CompressedNode> map;

	public NodeMap() {
		lst = new ArrayList<CompressedNode>();
		map = new HashMap<CompressedNode, CompressedNode>();
	}

	public synchronized CompressedNode get(Node n) throws IOException {
		if (n == null || n == Node.ANY) {
			return CompressedNode.ANY;
		}

		CompressedNode wild = new CompressedNode(n, BitConstants.WILD);
		CompressedNode cn = map.get(wild);

		if (cn == null) {
			cn = add(wild);
		}
		return cn;
	}

	public CompressedNode get(int idx) {
		return lst.get(idx);
	}

	private synchronized CompressedNode add(CompressedNode n) {
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
		out.writeInt( lst.size() );
		for (int i=0;i<lst.size();i++)
		{
			out.writeObject( lst.get(i) );
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int count = in.readInt();
		for (int i=0;i<count;i++)
		{
			CompressedNode cn = (CompressedNode) in.readObject();
			if (lst.size() <= cn.getIdx())
			{
				int fill = cn.getIdx()-lst.size()+1;
				lst.addAll( Arrays.asList(new CompressedNode[fill]) );
			}
			lst.set( cn.getIdx(), cn  );
			map.put( cn,  cn );
		}
	}
}
