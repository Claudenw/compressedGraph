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

import org.xenei.compressedgraph.BitCube.Idx;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;

public class CompressedGraph extends GraphBase {
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
		int s = map.get(t.getMatchSubject());
		int p = map.get(t.getMatchPredicate());
		int o = map.get(t.getMatchObject());
		return data.find(s, p, o).mapWith(new Map1<Idx, Triple>() {

			@Override
			public Triple map1(Idx idx) {
				Node s = map.get(idx.getX());
				Node p = map.get(idx.getY());
				Node o = map.get(idx.getZ());
				return new Triple(s, p, o);
			}
		});
	}

	@Override
	public void performAdd(Triple t) {
		int s = map.get(t.getSubject());
		int p = map.get(t.getPredicate());
		int o = map.get(t.getObject());
		data.set(s, p, o);
	}

	@Override
	protected int graphBaseSize() {
		return data.getSize();
	}

	@Override
	public void performDelete(Triple t) {
		int s = map.get(t.getSubject());
		int p = map.get(t.getPredicate());
		int o = map.get(t.getObject());
		data.clear(s, p, o);
	}

}
