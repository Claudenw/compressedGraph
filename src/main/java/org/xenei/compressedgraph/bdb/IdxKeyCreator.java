package org.xenei.compressedgraph.bdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

import org.xenei.compressedgraph.covert.CompressedNode;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class IdxKeyCreator implements SecondaryKeyCreator {

	public IdxKeyCreator() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
		if (data != null) {
            // Convert dataEntry to an Inventory object
			CompressedNode cn = new CompressedNode( data.getData() );
            result.setData( createBuffer( cn.getIdx() ));
        }
        return true;
	}

	public static byte[] createBuffer( int i )
	{
		 return ByteBuffer.allocate(4).putInt( i ).array();
	}
}
