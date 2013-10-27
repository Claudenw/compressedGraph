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
package org.xenei.compressedgraph.bdb;

import java.io.IOException;
import org.xenei.compressedgraph.AbstractCompressedGraph;
import com.sleepycat.je.DatabaseException;

/**
 * Compressed graph using the berkley DB version
 * 
 */
public class CompressedGraph extends AbstractCompressedGraph {

	public CompressedGraph() throws DatabaseException,
			IllegalArgumentException, IOException {
		super(new NodeMap());
	}

	public CompressedGraph(int pageSize) throws DatabaseException,
			IllegalArgumentException, IOException {
		super(pageSize, new NodeMap());
	}
}
