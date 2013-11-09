package org.xenei.compressedgraph;

import java.io.IOException;

import com.hp.hpl.jena.graph.Node;

public interface EnumeratedNode {

	public static final int WILD = -1;

	public int getIdx();

	public byte getType();

	@Override
	public int hashCode();

	public byte[] getData() throws IOException;

	public Node getNode() throws IOException;

	public static class Util {
		public static boolean equals(EnumeratedNode n1, EnumeratedNode n2) {
			if (n1.hashCode() == n2.hashCode() && n1.getType() == n2.getType()) {

				try {
					return n1.getNode().equals(n2.getNode());
				} catch (IOException e) {
					return false;
				}
			}
			return false;
		}
	}

}
