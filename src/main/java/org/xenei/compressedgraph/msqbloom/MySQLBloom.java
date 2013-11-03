package org.xenei.compressedgraph.msqbloom;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.compressedgraph.bloom.BloomCapabilities;
import org.xenei.compressedgraph.bloom.BloomGraph;

import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

/**
 * Table 
 * create table bloom ( bloomId int unsigned not null, data blob , index blm_idx ( bloomId ) );
 *
 */
public class MySQLBloom implements BloomCapabilities {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLBloom.class);
	
	private Connection conn = null;
	private PreparedStatement insert;
	private PreparedStatement delete;
	private PreparedStatement findExact;
	private PreparedStatement findBloom;
	private PreparedStatement count;

	// "jdbc:mysql://localhost/test?user=%s&password=%s"
	public MySQLBloom( String url ) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		conn =  DriverManager.getConnection( url );
		insert = conn.prepareStatement( "INSERT INTO bloom SET bloomId=?, data=?" );
		delete = conn.prepareStatement( "DELETE FROM bloom WHERE bloomId=? AND data=?");
		findExact = conn.prepareStatement( "SELECT data FROM bloom WHERE bloomId=?");
		findBloom = conn.prepareStatement( "SELECT data FROM bloom WHERE bloomId>? && ((bloomId & ?) = ?)");
		count = conn.prepareStatement( "SELECT count(*) FROM bloom");
	}

	@Override
	public boolean supportsFullBloom() {
		return true;
	}

	@Override
	public boolean supportsExact() {
		return true;
	}

	@Override
	public boolean canBeEmpty() {
		return true;
	}

	@Override
	public boolean sizeAccurate() {
		return true;
	}

	@Override
	public int getSize() {
		
		try {
			ResultSet rs = count.executeQuery();
			long count = rs.getLong(1);
			if (count > Integer.MAX_VALUE)
			{
				return Integer.MAX_VALUE;
			}
			return (int) count;
		}
		catch (SQLException e)
		{
			return Integer.MAX_VALUE/2;
		}
	}

	@Override
	public boolean addAllowed() {
		return true;
	}

	@Override
	public boolean addsDuplicates() {
		return true;
	}
	
	private void closeQuietly( ResultSet rs )
	{
		if (rs != null)
		{
			try {
				rs.close();
			} catch (SQLException e)
			{
				LOG.warn( e.getMessage() );
			}
		}
	}
	
	@Override
	public void write(byte[] bloomValue, ByteBuffer data) {
		

		try {
			Blob b = new SerialBlob( data.array());
			
			insert.setLong(1, BloomGraph.getBloomValue(bloomValue));
			insert.setBlob( 2, b );
			insert.executeUpdate();
		} catch (SerialException e) {
			LOG.error( e.getMessage(), e );
		} catch (SQLException e) {
			LOG.error( e.getMessage(), e );
		}
	}

	@Override
	public boolean deleteAllowed() {
		return true;
	}

	@Override
	public void delete(byte[] bloomValue, ByteBuffer data) {
		try {
			Blob b = new SerialBlob( data.array());
			
			delete.setLong(1, BloomGraph.getBloomValue(bloomValue));
			delete.setBlob( 2, b );
			delete.executeUpdate();
		} catch (SerialException e) {
			LOG.error( e.getMessage(), e );
		} catch (SQLException e) {
			LOG.error( e.getMessage(), e );
		}
	}

	@Override
	public ExtendedIterator<ByteBuffer> find(byte[] bloomValue, boolean exact) {
		try {
			ResultSet rs = null;
			if (exact)
			{
				findExact.setLong( 1, BloomGraph.getBloomValue(bloomValue) );
				rs = findExact.executeQuery();
			}
			else
			{
				long l =  BloomGraph.getBloomValue(bloomValue);
				findBloom.setLong( 1, l );
				findBloom.setLong( 2, l );
				findBloom.setLong( 3, l );
				rs = findBloom.executeQuery();
			}
			return WrappedIterator.create( new RSIterator( rs ) );
		} catch (SQLException e) {
			LOG.error( e.getMessage(), e );
			return WrappedIterator.emptyIterator();
		}
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error( e.getMessage(), e  );
		}
	}

	private class RSIterator implements ClosableIterator<ByteBuffer>
	{
		ResultSet rs;
		
		RSIterator( ResultSet rs) throws SQLException
		{
			this.rs = rs;
			if (hasNext())
			{
				rs.next();
			}
		}

		@Override
		public boolean hasNext() {
			try {
				return !rs.isAfterLast();
			} catch (SQLException e) {
				LOG.error( e.getMessage(), e );
				return false;
			}
		}

		@Override
		public ByteBuffer next() {
			if (hasNext())
			{
				try {
				ByteBuffer retval = ByteBuffer.wrap( rs.getBytes(1));
				try {
					rs.next();
				} catch (SQLException e) {
					LOG.error( e.getMessage(), e );
				}
				return retval;
				
			} catch (SQLException e) {
				LOG.error( e.getMessage(), e );
				throw new RuntimeException( e );
			}
			} else {
				throw new NoSuchElementException();
			}
			
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException( "remove not supported");
		}

		@Override
		public void close() {
			closeQuietly( rs );
		}
	}

	
}
