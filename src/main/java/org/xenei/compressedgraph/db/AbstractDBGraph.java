package org.xenei.compressedgraph.db;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.compressedgraph.SerializableNode;
import org.xenei.compressedgraph.mfp.NodeMap.MapEntry;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

public class AbstractDBGraph extends GraphBase {

	private final static Logger LOG = LoggerFactory
			.getLogger(AbstractDBGraph.class);
	private DBCapabilities capabilities;
	
	private final static int MAX_ENTRIES = 100;
	private Map<Node, SerializableNode> nodeCache;

	public AbstractDBGraph(DBCapabilities capabilities) {
		this.capabilities = capabilities;
		nodeCache = Collections
				.synchronizedMap(new LinkedHashMap<Node, SerializableNode>(
						MAX_ENTRIES + 1, .75F, true) {
					// This method is called just after a new entry has been
					// added
					@Override
					public boolean removeEldestEntry(Map.Entry<Node, SerializableNode> eldest) {
						return size() > MAX_ENTRIES;
					}
				});
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		try {
			SerializableNode s = saveNode(m.getMatchSubject());
			SerializableNode p = saveNode(m.getMatchPredicate());
			SerializableNode o = saveNode(m.getMatchObject());
			return capabilities.find(s, p, o);
		} catch (IOException e) {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public void performAdd(Triple t) {
		try {
			SerializableNode s = saveNode(t.getSubject());
			SerializableNode p = saveNode(t.getPredicate());
			SerializableNode o = saveNode(t.getObject());
			capabilities.save(s, p, o);
		} catch (IOException e) {
			LOG.error(String.format("Unable to save %s", t), e);
		}
	}

	@Override
	protected int graphBaseSize() {
		return capabilities.getSize();
	}

	@Override
	public void performDelete(Triple t) {
		try {
			SerializableNode s = saveNode(t.getSubject());
			SerializableNode p = saveNode(t.getPredicate());
			SerializableNode o = saveNode(t.getObject());
			capabilities.delete(s, p, o);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private SerializableNode saveNode(Node n) throws IOException {
		if (n == null || n.equals(Node.ANY)) {
			return SerializableNode.ANY;
		}
		SerializableNode sn = nodeCache.get(n);
		if (sn == null )
		{
			sn = capabilities.register(n);
			nodeCache.put( n, sn);
		}
		return sn;
	}
}
