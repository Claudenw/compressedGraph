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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

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
public class SerializableNode extends StoredNode implements NodeTypes,
		Serializable, EnumeratedNode {

	private static final int MAX_STR_SIZE = 750;

	private static final int HASH_CODE_OFFSET = 0;
	private static final int IDX_OFFSET = 4;
	private static final int TYPE_OFFSET = 8;
	private static final int DATA_OFFSET = 9;
	public static final SerializableNode ANY;

	static {
		try {
			ANY = new SerializableNode(Node.ANY);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private transient ByteBuffer buffer;
	private byte[] value;

	public SerializableNode(byte[] serializedValue) {
		super(null);
		this.value = serializedValue;
	}

	public SerializableNode(Node n) throws IOException {
		super(n);
		if (n.equals(Node.ANY)) {
			// ANY has a hash code of 0
			fillBuffer(WILD, 0, _ANY, null);
		} else if (n.isVariable()) {
			fillBuffer(WILD, n.hashCode(), _VAR, encodeString(n.getName()));
		} else if (n.isURI()) {
			fillBuffer(WILD, n.hashCode(), _URI, encodeString(n.getURI()));
		} else if (n.isBlank()) {
			fillBuffer(WILD, n.hashCode(), _ANON, encodeString(n.getBlankNodeId()
					.getLabelString()));
		} else if (n.isLiteral()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream os = new DataOutputStream(baos);
			write(os, n.getLiteralLexicalForm());
			write(os, n.getLiteralLanguage());
			write(os, n.getLiteralDatatypeURI());

			os.close();
			baos.close();
			byte[] value = baos.toByteArray();
			if (value.length > MAX_STR_SIZE) {
				baos = new ByteArrayOutputStream();
				GZIPOutputStream dos = new GZIPOutputStream(baos);
				dos.write(value);
				dos.close();
				fillBuffer(WILD, n.hashCode(),(byte) (_LIT | _COMPRESSED) , baos.toByteArray());
			} else {
				fillBuffer(WILD, n.hashCode(), _LIT, value);
			}
		} else {
			throw new IllegalArgumentException("Unknown node type " + n);
		}
	}

	@Override
	public int getIdx() {
		return getByteBuffer().getInt(IDX_OFFSET);
	}

	public void setIdx(int idx) {
		if (getIdx() != EnumeratedNode.WILD) {
			throw new IllegalStateException(
					"CompressedNode id must be WILD to be set");
		}
		getByteBuffer().putInt(IDX_OFFSET, idx);
	}

	private ByteBuffer getByteBuffer() {
		if (buffer == null) {
			buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
		}
		return buffer;
	}

	protected void fillBuffer(int idx, int hashCode, byte type, byte[] buff) {
		value = new byte[DATA_OFFSET + (buff == null ? 0 : buff.length)];
		getByteBuffer().putInt(hashCode).putInt(idx).put(type);
		if (buff != null) {
			getByteBuffer().put(buff);
		}
	}

	private void write(DataOutputStream os, String s) throws IOException {
		if (s == null) {
			os.writeInt(-1);
		} else {
			byte[] b = encodeString(s);
			os.writeInt(b.length);
			if (b.length > 0) {
				os.write(b);
			}
		}
	}

	/**
	 * Return the entire encoded buffer
	 * 
	 * @return
	 */
	public byte[] getBuffer() {
		return value;
	}

	/**
	 * Return just the node data
	 * 
	 * @return
	 */
	@Override
	public byte[] getData() throws IOException {
		int size = getSize();
		byte[] retval = new byte[size];
		if (size > 0) {
			getByteBuffer().position(DATA_OFFSET);
			getByteBuffer().get(retval);
		}
		if ((getType() & _COMPRESSED) == _COMPRESSED)
		{
			DataInputStream input = null;
			ByteArrayOutputStream output = null;
			try {
				input = new DataInputStream(new BufferedInputStream(
	            		new GZIPInputStream(new ByteArrayInputStream(retval))));
				output = new ByteArrayOutputStream();
				IOUtils.copy(input, output);
				retval = output.toByteArray();
			}
			finally {
				IOUtils.closeQuietly( input );
				IOUtils.closeQuietly( output );
			}
		}
		return retval;
	}

	public int getSize() {
		return value.length - DATA_OFFSET;
	}

	public boolean isLiteral() {
		return (getType() & NodeTypes._LIT) == NodeTypes._LIT;
	}

	@Override
	public byte getType() {
		getByteBuffer().position(TYPE_OFFSET);
		return getByteBuffer().get();
	}

	/**
	 * Get the index as a byte
	 * 
	 * @param rawData
	 * @return
	 */
	public static byte[] getRawIdx(byte[] src) {
		byte[] retval = new byte[4];
		System.arraycopy(src, IDX_OFFSET, retval, 0, 4);
		return retval;
	}

	public static byte[] getRawIdx(int i) {
		byte[] retval = new byte[4];
		ByteBuffer.wrap(retval).order(ByteOrder.BIG_ENDIAN).putInt(i);
		return retval;
	}

	@Override
	public int hashCode() {
		getByteBuffer().position(HASH_CODE_OFFSET);
		return getByteBuffer().getInt();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof SerializableNode) {
			SerializableNode cn = (SerializableNode) o;
			if (hashCode() == cn.hashCode() && getType() == cn.getType()
					&& getSize() == cn.getSize()) {
				if (getSize() > 0) {
					cn.getByteBuffer().position(DATA_OFFSET);
					getByteBuffer().position(DATA_OFFSET);
					int i = getByteBuffer().compareTo(cn.getByteBuffer());
					if (i == 0) {
						return true;
					}
				} else {
					return true;
				}
			}
		}
		return false;
	}

	private String decodeString(byte[] b) {
		try {
			return new String(b, "UTF-8");
		} catch (UnsupportedEncodingException e) { // should not happen
			throw new RuntimeException(e);
		}
	}

	private String read(DataInputStream is) throws IOException {
		int n = is.readInt();
		if (n == -1) {
			return null;
		}
		byte[] b = new byte[n];
		if (n > 0) {
			is.read(b);
		}
		return decodeString(b);
	}

	@Override
	protected Node extractNode() throws IOException {

		Node lnode = null;
		byte type = getType();
		switch (type & 0x0F) {
		case _ANON:
			lnode = NodeFactory.createAnon(AnonId
					.create(decodeString(getData())));
			break;

		case _LIT:
			InputStream bais = new ByteArrayInputStream(getData());
			DataInputStream is = new DataInputStream(new BufferedInputStream(
					bais));
			String lex = read(is);
			String lang = StringUtils.defaultIfBlank(read(is), null);
			String dtURI = read(is);
			is.close();
			RDFDatatype dtype = StringUtils.isEmpty(dtURI) ? null : TypeMapper
					.getInstance().getTypeByName(dtURI);
			LiteralLabel ll = LiteralLabelFactory.create(lex, lang, dtype);
			lnode = NodeFactory.createLiteral(ll);
			break;

		case _URI:
			lnode = NodeFactory.createURI(decodeString(getData()));
			break;

		case _VAR:
			lnode = NodeFactory.createVariable(decodeString(getData()));
			break;

		case _ANY:
			lnode = Node.ANY;
			break;

		default:
			throw new RuntimeException(String.format(
					"Unable to parse node: %0o", type));
		}
		return lnode;
	}

	private byte[] encodeString(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) { // should not happen
			throw new RuntimeException(e);
		}
	}
	
//	private void writeObject(java.io.ObjectOutputStream out)
//		     throws IOException
//		     {
//		out.writeInt( value.length );
//		out.write( value );
//		     }
//	
//		 private void readObject(java.io.ObjectInputStream in)
//		     throws IOException, ClassNotFoundException
//		     {
//			 int length = in.readInt();
//			 value = new byte[length];
//			 in.read( value );
//			 super.
//			 
//		     }
}