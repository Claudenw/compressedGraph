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
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.xenei.compressedgraph.core.BitConstants;
import org.xenei.compressedgraph.core.BitCube;
import org.xenei.compressedgraph.core.BitCube.Idx;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;

public class CompressedGraph extends GraphBase implements Serializable {
	private BitCube data;
	private NodeMap map;

	public CompressedGraph() {
		this(BitConstants.DEFAULT_PAGE_SIZE);
	}

	public CompressedGraph(int pageSize) {
		data = new BitCube(pageSize);
		map = new NodeMap();
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch t) {
		try {
			int s = map.get(t.getMatchSubject()).getIdx();
			int p = map.get(t.getMatchPredicate()).getIdx();
			int o = map.get(t.getMatchObject()).getIdx();
			return data.find(s, p, o).mapWith(new Map1<Idx, Triple>() {

				@Override
				public Triple map1(Idx idx) {
					try {
						Node s = map.get(idx.getX()).getNode();
						Node p = map.get(idx.getY()).getNode();
						Node o = map.get(idx.getZ()).getNode();
						return new Triple(s, p, o);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void performAdd(Triple t) {
		try {
			int s = map.get(t.getSubject()).getIdx();
			int p = map.get(t.getPredicate()).getIdx();
			int o = map.get(t.getObject()).getIdx();
			data.set(s, p, o);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected int graphBaseSize() {
		return data.getSize();
	}

	@Override
	public void performDelete(Triple t) {
		try {
			int s = map.get(t.getSubject()).getIdx();
			int p = map.get(t.getPredicate()).getIdx();
			int o = map.get(t.getObject()).getIdx();
			data.clear(s, p, o);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeObject(map);
		out.writeObject(data);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		map = (NodeMap) in.readObject();
		data = (BitCube) in.readObject();

	}

}
