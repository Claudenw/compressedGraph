package org.xenei.compressedgraph;

import org.xenei.compressedgraph.BitCube.Idx;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;

public class CompressedGraph extends GraphBase {
	
	
	private BitCube data = new BitCube();
	private NodeMap map = new NodeMap();
	
	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch t) {
		int s = map.get(t.getMatchSubject());
		int p = map.get(t.getMatchPredicate());
		int o = map.get(t.getMatchObject());
		return data.find( s, p, o ).mapWith(new Map1<Idx,Triple>(){

			public Triple map1(Idx idx) {
				Node s = map.get(idx.getX());
				Node p = map.get(idx.getY());
				Node o = map.get(idx.getZ());
				return new Triple( s, p, o );
			}});
	}

	@Override
	public void performAdd(Triple t) {
		int s = map.get(t.getSubject());
		int p = map.get(t.getPredicate());
		int o = map.get(t.getObject());
		data.set( s, p, o );
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
