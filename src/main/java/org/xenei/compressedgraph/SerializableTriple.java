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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import com.hp.hpl.jena.graph.Triple;

public class SerializableTriple {

  private static final int HASH_CODE_OFFSET = 0;
  private static final int S_LENGTH = 4;
  private static final int P_LENGTH = 8;
  private static final int O_LENGTH = 12;
  private static final int S_POS = 16;
  public static final SerializableTriple ANY;

  private transient SoftReference<Triple> triple;
  private transient ByteBuffer buffer;
  private byte[] value;

  private transient SerializableNode s;
  private transient SerializableNode p;
  private transient SerializableNode o;

  static {
    ANY = new SerializableTriple(SerializableNode.ANY, SerializableNode.ANY,
        SerializableNode.ANY);
  }

  public SerializableTriple(SerializableNode s, SerializableNode p,
                            SerializableNode o) {
    fillBuffer(s, p, o);
  }
  
  public SerializableTriple( Triple t ) throws IOException
  {
    s = new SerializableNode( t.getSubject() );
    p = new SerializableNode( t.getPredicate() );
    o = new SerializableNode( t.getObject() );
    fillBuffer( s, p ,o );
  }
  
  public SerializableTriple( byte[] value )
  {
    this.value = value;
    this.triple = null;
  }

  public SerializableTriple(ByteBuffer bytes) {
    value = new byte[ bytes.capacity() ];
    bytes.position(0);
    bytes.get(value);
    this.triple = null;

  }

  public boolean containsWild()
  {
    return getSubject().equals(SerializableNode.ANY) ||
        getPredicate().equals( SerializableNode.ANY ) ||
        getObject().equals(SerializableNode.ANY);
        
  }
  private void fillBuffer(SerializableNode s, SerializableNode p,
      SerializableNode o) {
    int hashCode = s.hashCode() | p.hashCode() | o.hashCode();
    int dataLen = s.getBuffer().length + p.getBuffer().length
        + o.getBuffer().length;
    value = new byte[dataLen + S_POS];
    buffer = getByteBuffer();
    buffer.position(0);
    buffer.putInt(hashCode).putInt(s.getBuffer().length)
        .putInt(p.getBuffer().length).putInt(o.getBuffer().length)
        .put(s.getBuffer()).put(p.getBuffer()).put(o.getBuffer());
    this.s = s;
    this.p = p;
    this.o = o;
  }

  public ByteBuffer getByteBuffer() {
    if (buffer == null) {
      buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
    }
    return buffer;
  }

  public SerializableNode getSubject() {
    if (s == null) {
      ByteBuffer buff = getByteBuffer();
      buff.position(S_LENGTH);
      byte[] sBuff = new byte[buff.getInt()];
      buff.position(S_POS);
      buff.get(sBuff);
      s = new SerializableNode(sBuff);
    }
    return s;
  }

  public SerializableNode getPredicate() {
    if (p == null) {
      ByteBuffer buff = getByteBuffer();
      buff.position(S_LENGTH);
      int offset = S_POS + buff.getInt(); // read s_length
      byte[] pBuff = new byte[buff.getInt()]; // read p_length
      getByteBuffer().position(offset);
      getByteBuffer().get(pBuff);
      p = new SerializableNode(pBuff);
    }
    return p;
  }

  public SerializableNode getObject() {
    if (o == null) {
      ByteBuffer buff = getByteBuffer();
      buff.position(S_LENGTH);
      int offset = S_POS + buff.getInt() + buff.getInt(); // read s_length &&
                                                          // p_lenfth
      byte[] pBuff = new byte[buff.getInt()]; // read o_length
      getByteBuffer().position(offset);
      getByteBuffer().get(pBuff);
      o = new SerializableNode(pBuff);
    }
    return o;
  }

  public Triple getTriple() throws IOException {
    Triple retval = null;
    if (triple != null) {
      retval = triple.get();
    }
    if (retval == null) {
      retval = new Triple(getSubject().getNode(), getPredicate().getNode(),
          getObject().getNode());
      triple = new SoftReference<Triple>(retval);
    }
    return retval;
  }

  @Override
  public int hashCode() {
    return getByteBuffer().getInt(HASH_CODE_OFFSET);
  }

  public int getSize() {
    return value.length;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SerializableTriple) {
      SerializableTriple cn = (SerializableTriple) o;
      if (hashCode() == cn.hashCode() && getSize() == cn.getSize()) {
        if (getSize() > 0) {
          cn.getByteBuffer().position(S_POS);
          getByteBuffer().position(S_POS);
          int i = getByteBuffer().compareTo(cn.getByteBuffer());
          if (i == 0) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
