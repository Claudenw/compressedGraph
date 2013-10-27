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
package org.xenei.compressedgraph;

import java.io.IOException;
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

/**
 * Abstract compressed graph.
 * 
 * The idea here is that nodes are converted into integers and the integers are
 * used to represent the nodes in the triple store. The big problem seemed to be
 * the mapping between integers and nodes. Though there are also issues with the
 * size of the sparse cube as more triples are added.
 * 
 * Several storage strategies are provided.
 * 
 */
public abstract class AbstractCompressedGraph extends GraphBase implements
		Serializable {
	private BitCube data;
	private INodeMap map;

	protected AbstractCompressedGraph(INodeMap map) {
		this(BitConstants.DEFAULT_PAGE_SIZE, map);
	}

	protected AbstractCompressedGraph(int pageSize, INodeMap map) {
		data = new BitCube(pageSize);
		this.map = map;
	}

	@Override
	public void close() {
		super.close();
		map.close();
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
		map = (INodeMap) in.readObject();
		data = (BitCube) in.readObject();
	}

}
