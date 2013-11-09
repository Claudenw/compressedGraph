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
	// private final String schema;
	// private final String tripleTbl;
	// private final String literalTbl;
	// private final String nodeTbl;

	private final Connection connection;

	private final PreparedStatement ps_count;
	private final PreparedStatement ps_delete;
	private final PreparedStatement ps_saveTriple;
	private final PreparedStatement ps_saveNode;
	private final PreparedStatement ps_nodeSearch;

	// private final PreparedStatement ps_saveLiteral;
	// private final PreparedStatement ps_literalSearch;
	// private final PreparedStatement ps_literalDeleteSearch;
	// private final PreparedStatement ps_literalUpdate;

	private final PreparedStatement ps_spo;
	// private final PreparedStatement ps_spoLit;
	private final PreparedStatement ps_sp;
	private final PreparedStatement ps_so;
	// private final PreparedStatement ps_soLit;
	private final PreparedStatement ps_s;
	private final PreparedStatement ps_po;
	// private final PreparedStatement ps_poLit;
	private final PreparedStatement ps_p;
	private final PreparedStatement ps_o;
	// private final PreparedStatement ps_oLit;
	private final PreparedStatement ps_all;

	public MySQLCapabilities(Connection connection, String schema,
			String triples, String nodes) throws SQLException {
		this.connection = connection;

		String selectPfx = "SELECT s.text, p.text, o.text FROM %1$s.%2$s t, %1$s.%3$s s, %1$s.%3$s p, %1$s.%3$s o "
				+ "WHERE t.subject=s.idx AND t.predicate=p.idx AND t.predicate=o.idx %%1$s ";

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
				"INSERT INTO %s.%s (type,hash,type,text) VALUES (?,?,?,?)",
				schema, nodes));
		ps_nodeSearch = connection.prepareStatement(String.format(
				"SELECT idx, text FROM %s.%s WHERE type=? AND text=?", schema,
				nodes));

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

	private boolean blobEquals(Blob b1, Blob b2) throws SQLException {
		if (b1.length() == b2.length()) {
			long pos = 0;
			while (pos <= b1.length()) {
				if (!Arrays.equals(b1.getBytes(pos, 1024),
						b2.getBytes(pos, 1024))) {
					return false;
				}
				pos += 1024;
			}
		}
		return false;
	}

	private boolean findNode(PreparedStatement ps, SerializableNode sn, Blob b1) {
		ResultSet rs = null;

		try {
			ps.setByte(1, sn.getType());
			ps.setBlob(2, b1);
			rs = ps.executeQuery();

			while (rs.next()) {
				Blob b2 = rs.getBlob(2);
				if (blobEquals(b1, b2)) {
					sn.setIdx(rs.getInt(1));
					return true;
				}
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

		if (node.isVariable()) {
			throw new IOException("variable nodes many not be stored in db");
		}

		// user execution sets and LAST_INSERT_ID() to get
		SerializableNode sn = new SerializableNode(node);
		ResultSet rs = null;
		try {
			Blob b = new SerialBlob(sn.getData());
			if (!findNode(ps_nodeSearch, sn, b)) {
				ps_saveNode.setByte(1, sn.getType());
				ps_saveNode.setBlob(1, b);
				ps_saveNode.execute();

				rs = ps_nodeSearch.executeQuery();

				if (rs.next()) {
					sn.setIdx(rs.getInt(1));
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
		if (s.isLiteral() || p.isLiteral()) {
			throw new IllegalArgumentException("S and P must not be Literal");
		}

		try {
			ps_saveTriple.setInt(1, s.getIdx());
			ps_saveTriple.setInt(2, p.getIdx());
			ps_saveTriple.setByte(3, (byte) (o.isLiteral() ? 1 : 0));
			ps_saveTriple.setInt(4, o.getIdx());
		} catch (SQLException e) {
			LOG.warn("Error saving literal triple", e);
		}
	}

	@Override
	public void delete(SerializableNode s, SerializableNode p,
			SerializableNode o) {
		if (s.isLiteral() || p.isLiteral()) {
			throw new IllegalArgumentException("S and P must not be WILD");
		}

		try {
			ps_delete.setInt(1, s.getIdx());
			ps_delete.setInt(2, p.getIdx());
			ps_delete.setByte(3, (byte) (o.isLiteral() ? 1 : 0));
			ps_delete.setInt(4, o.getIdx());
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
		int hash = s.hashCode() | p.hashCode() | o.hashCode();

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
		ps_s.setInt(2, s.getIdx());
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
		ps_p.setInt(2, p.getIdx());
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
						SerializableNode s = new SerializableNode(b.getBytes(
								b.length(), 0));
						b = rs.getBlob(2);
						SerializableNode p = new SerializableNode(b.getBytes(
								b.length(), 0));
						b = rs.getBlob(3);
						SerializableNode o = new SerializableNode(b.getBytes(
								b.length(), 0));
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
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}

/*
 * 
 * CREATE FUNCTION addLiteral( int, byte type int hash blob
 */