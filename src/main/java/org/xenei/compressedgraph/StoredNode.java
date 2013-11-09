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
import java.lang.ref.SoftReference;
import com.hp.hpl.jena.graph.Node;

/**
 * A binary representation of a Node. Includes the mapping of the node to an
 * index value. For literal node types long lexical values are compressed for
 * storage.
 * 
 * Will return the Node value. Keeps a soft reference to the node so that it may
 * be garbage collected if necessary.
 * 
 * I serializable so that it can be written to a stream if necessary.
 * 
 */
public abstract class StoredNode implements NodeTypes, EnumeratedNode {

	public static final StoredNode ANY;

	static {

		ANY = new StoredNode(Node.ANY) {
			@Override
			public int hashCode() {
				return 0;
			}

			@Override
			public byte getType() {
				return _ANY;
			}

			@Override
			public boolean equals(Object o) {
				if (o instanceof StoredNode) {
					return ((StoredNode) o).getType() == _ANY;
				}
				return false;
			}

			@Override
			protected Node extractNode() {
				return Node.ANY;
			}

			@Override
			public int getIdx() {
				return -1;
			}

			@Override
			public byte[] getData() {
				return new byte[0];
			}
		};

	}

	private transient SoftReference<Node> node;

	public StoredNode() {
		this(null);
	}
	public StoredNode(Node n) {
		if (n != null) {
			this.node = new SoftReference<Node>(n);
		}
	}

	@Override
	public abstract byte getType();

	@Override
	public boolean equals(Object o) {
		if (o instanceof EnumeratedNode) {
			return EnumeratedNode.Util.equals(this, (EnumeratedNode) o);
		}
		return false;
	}

	@Override
	public final Node getNode() throws IOException {
		Node retval = null;
		if (node != null) {
			retval = node.get();
		}
		if (retval == null) {
			retval = extractNode();
			node = new SoftReference<Node>(retval);
		}
		return retval;
	}

	protected abstract Node extractNode() throws IOException;

}