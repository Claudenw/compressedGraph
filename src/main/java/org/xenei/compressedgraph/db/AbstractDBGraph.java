package org.xenei.compressedgraph.db;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.compressedgraph.SerializableNode;

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

	public AbstractDBGraph(DBCapabilities capabilities) {
		this.capabilities = capabilities;
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
		return capabilities.register(n);
	}
}
