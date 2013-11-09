package org.xenei.compressedgraph.db;

import java.io.IOException;
import org.xenei.compressedgraph.SerializableNode;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public interface DBCapabilities {

	SerializableNode register(Node node) throws IOException;

	void save(SerializableNode s, SerializableNode p, SerializableNode o);

	void delete(SerializableNode s, SerializableNode p, SerializableNode o);

	int getSize();

	ExtendedIterator<Triple> find(SerializableNode s, SerializableNode p,
			SerializableNode o);

}
