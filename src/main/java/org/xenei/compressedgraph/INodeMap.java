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
import java.nio.ByteBuffer;

import org.xenei.compressedgraph.covert.CompressedNode;

import com.hp.hpl.jena.graph.Node;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public interface INodeMap extends Serializable {

	public CompressedNode get(Node n) throws IOException;

	public CompressedNode get(int idx) throws IOException;
	
	public void close();
	
	public int count();
	
	public void add( CompressedNode cn );
	
//	public static class Util {
//		public static byte[] toStore( CompressedNode cn )
//		{
//			return ByteBuffer.allocateDirect(4+2+cn.getSize())
//				.putInt( cn.getIdx() )
//				.putChar( cn.getType() )
//				.put(cn.getData())
//				.array();
//		}
//	}

}
