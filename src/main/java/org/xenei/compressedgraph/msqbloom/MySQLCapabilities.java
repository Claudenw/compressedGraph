package org.xenei.compressedgraph.msqbloom;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.NoSuchElementException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.compressedgraph.EnumeratedNode;
import org.xenei.compressedgraph.SerializableNode;
import org.xenei.compressedgraph.db.DBCapabilities;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class MySQLCapabilities implements DBCapabilities {

	private static final Logger LOG = LoggerFactory
			.getLogger(MySQLCapabilities.class);

	private final Connection connection;

	private final PreparedStatement ps_count;
	private final PreparedStatement ps_delete;
	private final PreparedStatement ps_saveTriple;
	private final PreparedStatement ps_saveNode;
	private final PreparedStatement ps_lastIdx;
	private final PreparedStatement ps_nodeSearch;

	private final PreparedStatement ps_spo;
	private final PreparedStatement ps_sp;
	private final PreparedStatement ps_so;
	private final PreparedStatement ps_s;
	private final PreparedStatement ps_po;
	private final PreparedStatement ps_p;
	private final PreparedStatement ps_o;
	private final PreparedStatement ps_all;

	public MySQLCapabilities(Connection connection, String schema,
			String triples, String nodes) throws SQLException {
		this.connection = connection;

		String selectPfx = "SELECT s.data, p.data, o.data FROM %1$s.%2$s t, %1$s.%3$s s, %1$s.%3$s p, %1$s.%3$s o "
				+ "WHERE t.subject=s.idx AND t.predicate=p.idx AND t.object=o.idx %%1$s ";

		selectPfx = String.format(selectPfx, schema, triples, nodes);

		ps_count = connection.prepareStatement(String.format(
				"SELECT count(*) FROM %s.%s", schema, triples));
		ps_delete = connection
				.prepareStatement(String
						.format("DELETE FROM %s.%s WHERE subject=? AND predicate=? AND object=?",
								schema, triples));
		ps_saveTriple = connection.prepareStatement(String.format(
				"INSERT INTO %s.%s (subject,predicate,object) VALUES (?,?,?)",
				schema, triples));
		ps_saveNode = connection.prepareStatement(String.format(
				"INSERT INTO %s.%s (data) VALUES (?)",
				schema, nodes));
		ps_lastIdx = connection.prepareStatement("SELECT LAST_INSERT_ID()");
		
//		ps_nodeSearch = connection.prepareStatement(String.format(
//				"SELECT idx, data FROM %s.%s WHERE data=?", schema,
//				nodes));

		ps_nodeSearch = connection.prepareStatement(String.format(
				"SELECT %s.register_node( ? )", schema ));
		
		
//				nodes));
		ps_spo = connection.prepareStatement(String.format(selectPfx,
				" AND subject=? AND predicate=? AND object=?"));

		ps_sp = connection.prepareStatement(String.format(selectPfx,
				" AND subject=? AND predicate=?"));

		ps_so = connection.prepareStatement(String.format(selectPfx,
				" AND subject=? AND object=?"));

		ps_s = connection.prepareStatement(String.format(selectPfx,
				" AND subject=?"));

		ps_po = connection.prepareStatement(String.format(selectPfx,
				" AND predicate=? AND object=?"));

		ps_p = connection.prepareStatement(String.format(selectPfx,
				" AND predicate=?"));

		ps_o = connection.prepareStatement(String.format(selectPfx,
				" AND object=?"));

		ps_all = connection.prepareStatement(String.format(selectPfx, " "));

	}

	private static byte[] getBlobData(Blob b) throws SQLException {
		int len = (int) b.length();
		return b.getBytes(1, len);
	}
	
	private boolean blobEquals(Blob b1, Blob b2) throws SQLException {
		if (b1.length() == b2.length()) {
			if (b1.length() == 0)
			{
				return true;
			}
			return Arrays.equals(getBlobData(b1), getBlobData(b2));
		}
		return false;
	}

	@SuppressWarnings("resource")
	private boolean findNode(SerializableNode sn, Blob b1) {
		ResultSet rs = null;
		 
		try {
			ps_nodeSearch.setBlob(1, b1);
			rs = ps_nodeSearch.executeQuery();

			if (rs.next()) {
				sn.setIdx(rs.getInt(1));
				return true;
			}
			return false;
		} catch (SQLException e) {
			LOG.warn("Error finding node", e);
			return false;
		} finally {
			DbUtils.closeQuietly(rs);
		}
	}

	@Override
	public SerializableNode register(Node node) throws IOException {
		SerializableNode sn = new SerializableNode(node);
		ResultSet rs = null;
		try {
			Blob b = new SerialBlob(sn.getBuffer());
			if (!findNode(sn, b)) {
				ps_saveNode.setBlob(1, b);
				ps_saveNode.executeUpdate();
				rs = ps_lastIdx.executeQuery();
				
			    if (rs.next()) {
			        sn.setIdx(rs.getInt(1));
			    } else {
			    	LOG.error("Unable to retrieve new node id "+sn);
					throw new IOException( "Unable to retrieve new node id");
			    }
			}
		} catch (SerialException e) {
			LOG.error("Unable to write blob", e);
			throw new IOException("Unable to write blob", e);
		} catch (SQLException e) {
			LOG.error("Unable to write node", e);
			throw new IOException("Unable to write node", e);
		}

		finally {
			DbUtils.closeQuietly(rs);
		}

		return sn;
	}

	@Override
	public void save(SerializableNode s, SerializableNode p, SerializableNode o) {
		try {
			ps_saveTriple.setInt(1, s.getIdx());
			ps_saveTriple.setInt(2, p.getIdx());
			ps_saveTriple.setInt(3, o.getIdx());
			ps_saveTriple.execute();
		} catch (SQLException e) {
			LOG.warn("Error saving literal triple", e);
		}
	}

	@Override
	public void delete(SerializableNode s, SerializableNode p,
			SerializableNode o) {
		
		try {
			ps_delete.setInt(1, s.getIdx());
			ps_delete.setInt(2, p.getIdx());
			ps_delete.setInt(3, o.getIdx());
		} catch (SQLException e) {
			LOG.warn("Error saving literal triple", e);
		}
	}

	@Override
	public int getSize() {
		ResultSet rs = null;
		try {
			rs = ps_count.executeQuery();
			if (!rs.next()) {
				return 0;
			}
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		} finally {
			DbUtils.closeQuietly(rs);
		}
	}

	private boolean isDefined(SerializableNode n) {
		if (n.getIdx() == EnumeratedNode.WILD) {
			return false;
		}
		if (n.equals(SerializableNode.ANY)) {
			return false;
		}
		return true;
	}

	@Override
	public ExtendedIterator<Triple> find(SerializableNode s,
			SerializableNode p, SerializableNode o) {
		
		PreparedStatement stmt = null;
		try {
			if (isDefined(s)) {
				if (isDefined(p)) {
					if (isDefined(o)) {
						stmt = do_SPO(s, p, o);
					} else {
						stmt = do_SP(s, p);
					}
				} else {
					if (isDefined(o)) {
						stmt = do_SO(s, o);
					} else {
						stmt = do_S(s);
					}

				}
			} else {
				if (isDefined(p)) {
					if (isDefined(o)) {
						stmt = do_PO(p, o);
					} else {
						stmt = do_P(p);
					}
				} else {
					if (isDefined(o)) {
						stmt = do_O(o);
					} else {
						stmt = ps_all;
					}

				}
			}
			return WrappedIterator.create(new TripleIterator(stmt
					.executeQuery()));
		} catch (SQLException e) {
			return NiceIterator.emptyIterator();
		}
	}

	private PreparedStatement do_SPO(SerializableNode s, SerializableNode p,
			SerializableNode o) throws SQLException {

		ps_spo.setInt(1, s.getIdx());
		ps_spo.setInt(2, p.getIdx());
		ps_spo.setInt(3, o.getIdx());
		return ps_spo;

	}

	private PreparedStatement do_SP(SerializableNode s, SerializableNode p)
			throws SQLException {
		ps_sp.setInt(1, s.getIdx());
		ps_sp.setInt(2, p.getIdx());
		return ps_sp;
	}

	private PreparedStatement do_SO(SerializableNode s, SerializableNode o)
			throws SQLException {
		ps_so.setInt(1, s.getIdx());
		ps_so.setInt(2, o.getIdx());
		return ps_so;
	}

	private PreparedStatement do_S(SerializableNode s) throws SQLException {
		ps_s.setInt(1, s.getIdx());
		return ps_s;
	}

	private PreparedStatement do_PO(SerializableNode p, SerializableNode o)
			throws SQLException {
		ps_po.setInt(1, p.getIdx());
		ps_po.setInt(2, o.getIdx());
		return ps_po;

	}

	private PreparedStatement do_P(SerializableNode p) throws SQLException {
		ps_p.setInt(1, p.getIdx());
		return ps_p;
	}

	private PreparedStatement do_O(SerializableNode o) throws SQLException {
		ps_o.setInt(1, o.getIdx());
		return ps_o;

	}

	private static class TripleIterator implements ClosableIterator<Triple> {
		private ResultSet rs;
		private Triple next;
		private boolean used;

		public TripleIterator(ResultSet rs) {
			this.rs = rs;
			next = null;
			used = true;
		}

		@Override
		public void close() {
			DbUtils.closeQuietly(rs);
		}

		@Override
		public boolean hasNext() {
			if (used) {
				try {
					if (rs.next()) {
						Blob b = rs.getBlob(1);
						SerializableNode s = new SerializableNode(getBlobData(b));
						b = rs.getBlob(2);
						SerializableNode p = new SerializableNode(getBlobData(b));
						b = rs.getBlob(3);
						SerializableNode o = new SerializableNode(getBlobData(b));
						next = new Triple(s.getNode(), p.getNode(), o.getNode());
						used = false;
					} else {
						used = false;
						next = null;
					}
				} catch (SQLException e) {
					used = false;
					next = null;
				} catch (IOException e) {
					used = false;
					next = null;
				}
			}
			return next != null;
		}

		@Override
		public Triple next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			used=true;
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
