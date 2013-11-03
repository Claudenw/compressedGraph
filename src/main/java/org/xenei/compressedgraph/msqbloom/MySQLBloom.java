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
import java.util.Arrays;
import java.util.NoSuchElementException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

/**
 * Table create table bloom ( bloomId int unsigned not null, data blob , index
 * blm_idx ( bloomId ) );
 * 
 */
public class MySQLBloom implements BloomCapabilities {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLBloom.class);

	private Connection conn = null;
	private PreparedStatement insert;
	private PreparedStatement delete;
	private PreparedStatement find;
	private PreparedStatement count;
	private PreparedStatement maxValue;

	// "jdbc:mysql://localhost/test?user=%s&password=%s"
	public MySQLBloom(String url, String table) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		conn = DriverManager.getConnection(url);
		insert = conn
				.prepareStatement(String.format("INSERT INTO %s SET bloomId=?, data=?", table));
		delete = conn
				.prepareStatement(String.format("DELETE FROM %s WHERE bloomId=? AND data=?", table));
		find = conn
				.prepareStatement(String.format("SELECT data FROM %s WHERE bloomId=?", table));
		count = conn.prepareStatement(String.format("SELECT count(*) FROM %s", table));
		maxValue = conn.prepareStatement(String.format("SELECT max(bloomId) from %s",table));
	}

	@Override
	public boolean supportsBloomQuery() {
		return false;
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
			if (count > Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			}
			return (int) count;
		} catch (SQLException e) {
			return Integer.MAX_VALUE / 2;
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

	private void closeQuietly(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				LOG.warn(e.getMessage());
			}
		}
	}

	@Override
	public void write(ByteBuffer bloomValue, ByteBuffer data) {
		try {
			Blob k = new SerialBlob(bloomValue.array());
			Blob b = new SerialBlob(data.array());
			insert.setBlob(1, k);
			insert.setBlob(2, b);
			insert.executeUpdate();
		} catch (SerialException e) {
			LOG.error(e.getMessage(), e);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public ByteBuffer getMaxBloomValue() {
		ResultSet rs = null;
		try {
			rs = maxValue.executeQuery();
			rs.next();
			long l = rs.getLong(1);
			return BloomGraph.encodeBloomValue(l);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			byte[] val = new byte[4];
			Arrays.fill(val, (byte) 0xFF);
			return ByteBuffer.wrap(val);
		} finally {
			closeQuietly(rs);
		}
	}

	@Override
	public boolean deleteAllowed() {
		return true;
	}

	@Override
	public void delete(ByteBuffer bloomValue, ByteBuffer data) {
		try {
			Blob k = new SerialBlob(bloomValue.array());
			Blob b = new SerialBlob(data.array());
			delete.setBlob(1, k);
			delete.setBlob(2, b);
			delete.executeUpdate();
		} catch (SerialException e) {
			LOG.error(e.getMessage(), e);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public ExtendedIterator<ByteBuffer> find(ByteBuffer bloomValue,
			boolean exact) {
		ResultSet rs = null;
		try {
			if (exact) {
				Blob k = new SerialBlob(bloomValue.array());
				find.setBlob(1, k);
				rs = find.executeQuery();
				return WrappedIterator.create(new RSIterator(rs));
			} else {
				throw new UnsupportedOperationException(
						"Bloom query not supported");
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			closeQuietly(rs);
			return WrappedIterator.emptyIterator();
		}
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private class RSIterator implements ClosableIterator<ByteBuffer> {
		private ResultSet rs;
		private boolean done;

		RSIterator(ResultSet rs) throws SQLException {
			this.rs = rs;
			done = !rs.next();
		}

		@Override
		public boolean hasNext() {
			return !done;
		}

		@Override
		public ByteBuffer next() {
			if (hasNext()) {
				try {
					ByteBuffer retval = ByteBuffer.wrap(rs.getBytes(1));
					try {
						done = !rs.next();
					} catch (SQLException e) {
						LOG.error(e.getMessage(), e);
					}
					return retval;

				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
					throw new RuntimeException(e);
				}
			} else {
				throw new NoSuchElementException();
			}

		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}

		@Override
		public void close() {
			closeQuietly(rs);
		}
	}

}
