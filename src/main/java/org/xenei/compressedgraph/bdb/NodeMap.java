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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.VersionMismatchException;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;

import org.xenei.compressedgraph.core.BitConstants;
import org.xenei.compressedgraph.covert.CompressedNode;

import com.hp.hpl.jena.graph.Node;

public class NodeMap implements Serializable {
	private Environment myDbEnvironment = null;
	private Database myDatabase = null;
	private SecondaryDatabase myIdxIndex;
	private StoredClassCatalog classCatalog;
	

	public NodeMap() throws EnvironmentNotFoundException, EnvironmentLockedException, VersionMismatchException, DatabaseException, IllegalArgumentException, IOException {

		// Open the environment. Create it if it does not already exist.
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    File f = Files.createTempDirectory("cgdb").toFile();
	    myDbEnvironment = new Environment( f, 
	                                      envConfig);

	    // Open the database. Create it if it does not already exist.
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    // Make it a temporary database
	    dbConfig.setTemporary(true);
	    myDatabase = myDbEnvironment.openDatabase(null, 
	                                              "CompressedGraph", 
	                                              dbConfig); 
	    
	    IdxKeyCreator keyCreator = new IdxKeyCreator();
	    
	    SecondaryConfig mySecConfig = new SecondaryConfig();
	    mySecConfig.setAllowCreate(true);
	    mySecConfig.setTemporary(true);
	    
	 // Set up the secondary properties
        mySecConfig.setAllowPopulate(true); // Allow autopopulate
        mySecConfig.setKeyCreator(keyCreator);
        // Need to allow duplicates for our secondary database
        mySecConfig.setSortedDuplicates(true);

        // Now open it
        myIdxIndex =
        		myDbEnvironment.openSecondaryDatabase(
                    null,     
                    "idxIndex", // Index name
                    myDatabase,     // Primary database handle. This is
                                     // the db that we're indexing. 
                    mySecConfig);    // The secondary config
	    
	}

	public void close()
	{
		try {      
	        if (myIdxIndex != null) {
	        	myIdxIndex.close();
	        }
	        
	        if (myDatabase != null) {
	            myDatabase.close();
	        }
	        
	        if (myDbEnvironment != null) {
	            myDbEnvironment.close();
	        }
	} catch (DatabaseException dbe) {
	    throw new RuntimeException( dbe );
	} 
	}
	
	private byte[] getKey( CompressedNode node )
	{
		byte[] retval = new byte[5+node.getSize()];
		ByteBuffer.wrap( retval ).order( ByteOrder.LITTLE_ENDIAN )
				.putInt( node.hashCode() ).put( node.getType ())
				.put( node.getData() );
		return retval;
	}
	
	public synchronized CompressedNode get(Node n) throws IOException {
		if (n == null || n == Node.ANY) {
			return CompressedNode.ANY;
		}

		CompressedNode wild = new CompressedNode(n, BitConstants.WILD);
		DatabaseEntry key = new DatabaseEntry( getKey(wild) );
		DatabaseEntry data = new DatabaseEntry();
		OperationStatus result = myDatabase.get(null, key, data, LockMode.DEFAULT);
		if (result == OperationStatus.NOTFOUND)
		{
			long l = myDatabase.count();
			wild.setIdx((int)l);
	
			data.setData( wild.getBuffer() );
			myDatabase.put(null, key, data);
			return wild;
		} else {
			return new CompressedNode( data.getData() );
		}
	}

	public CompressedNode get(int idx) {
		DatabaseEntry key = new DatabaseEntry( IdxKeyCreator.createBuffer( idx ));
		DatabaseEntry data = new DatabaseEntry();
		OperationStatus result = myIdxIndex.get(null, key, data, LockMode.DEFAULT);
		if (result == OperationStatus.NOTFOUND)
		{
			return null;
		}
		else
		{
			return new CompressedNode( data.getData() );
		}
	}
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		Cursor cursor = null;
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();
		try {
		   out.write( (int) myDatabase.count() );
		    cursor = myDatabase.openCursor(null, null);
		    while (cursor.getNext( foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		    {
		    	out.writeObject( new CompressedNode( foundData.getData()  ) );
		    }
		} catch (DatabaseException dbe) {
		    throw new IOException( dbe );
		} finally {
			if (cursor != null)
			{
				cursor.close();
			}
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		int count = in.readInt();
		for (int i=0;i<count;i++)
		{
			CompressedNode cn = (CompressedNode) in.readObject();
			key.setData(getKey(cn) );
			data.setData( cn.getBuffer() );
			if ( myDatabase.put(null, key, data) != OperationStatus.SUCCESS)
			{
				throw new IOException( "Unable to write data to db");
			}
		}
	}
}
