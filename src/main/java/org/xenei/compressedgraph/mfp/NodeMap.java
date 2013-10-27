package org.xenei.compressedgraph.mfp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.xenei.compressedgraph.CompressedNode;
import org.xenei.compressedgraph.INodeMap;
import com.hp.hpl.jena.graph.Node;

public class NodeMap implements INodeMap {

	private File dataDir;
	private Map<Integer, ListSet<Integer>> map;
	private int size;
	private final static int MAX_ENTRIES = 100;
	private Map<Integer, MapEntry> cache;
	private ExecutorService service = Executors.newSingleThreadExecutor();

	public NodeMap() throws IOException {
		File f = File.createTempFile("hmp", ".dat");

		dataDir = Files.createTempDirectory("mfp").toFile();

		String fn = f.getAbsolutePath();
		fn = fn.substring(0, fn.lastIndexOf('.')) + ".idx";
		map = new HashMap<Integer, ListSet<Integer>>();
		size = 0;
		cache = Collections
				.synchronizedMap(new LinkedHashMap<Integer, MapEntry>(
						MAX_ENTRIES + 1, .75F, true) {
					// This method is called just after a new entry has been
					// added
					@Override
					public boolean removeEldestEntry(Map.Entry eldest) {
						if (size() > MAX_ENTRIES) {
							Iterator<java.util.Map.Entry<Integer, MapEntry>> iter = this
									.entrySet().iterator();
							while (size() > MAX_ENTRIES && iter.hasNext()) {
								Entry<Integer, MapEntry> e = iter.next();
								if (e.getValue().done()) {
									// System.out.println( String.format(
									// "Removing entry: %H",
									// e.getValue().getNode().getIdx()));
									iter.remove();
								} else {
									// System.out.println( "Skipping entry");
								}
							}
						} else {
							// System.out.println("Skipping check: "+size());
						}
						return false;
					}
				});
	}

	private CompressedNode updateCache(CompressedNode cn) {
		cache.put(cn.getIdx(), new MapEntry(cn));
		return cn;
	}

	@Override
	public CompressedNode get(Node n) throws IOException {
		if (n == null || n == Node.ANY) {
			return CompressedNode.ANY;
		}

		for (MapEntry e : cache.values()) {
			if (n.equals(e.getNode().getNode())) {
				// force cache update
				cache.get(e.getNode().getIdx());
				return e.getNode();
			}
		}

		CompressedNode cn;
		ListSet<Integer> candidates = map.get(n.hashCode());

		if (candidates != null) {
			for (Integer i : candidates) {
				cn = read(i);
				if (n.equals(cn.getNode())) {
					return updateCache(cn);
				}
			}
		}

		cn = new CompressedNode(n, size);
		size++;
		if (size % 1000 == 0) {
			System.out.println("creating node " + size);
		}

		if (candidates == null) {
			candidates = new ListSet<Integer>();
			map.put(cn.hashCode(), candidates);
		}
		// write( cn );
		service.execute(new Writer(cn));
		candidates.add(cn.getIdx());
		return updateCache(cn);
	}

	@Override
	public CompressedNode get(int idx) throws IOException {
		return read(idx);
	}

	@Override
	public void close() {
		service.shutdown();
		try {
			// Wait a while for existing tasks to terminate
			if (!service.awaitTermination(60, TimeUnit.SECONDS)) {
				service.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!service.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			service.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public int count() {
		return size;
	}

	private File getFile(final int id) {
		// System.out.println( "Reading id: "+id );
		char[] c = String.format("%X", id).toCharArray();
		StringBuilder sb = new StringBuilder().append(c[0]);
		for (int i = 1; i < c.length; i++) {
			sb.append(System.getProperty("file.separator")).append(c[i]);
		}
		return new File(dataDir, sb.append(".dat").toString());
	}

	// private void write( CompressedNode cn ) throws IOException
	// {
	// File f = getFile( cn.getIdx() );
	// f.getParentFile().mkdirs();
	// FileOutputStream os = new FileOutputStream( f );
	// os.write( cn.getBuffer() );
	// os.close();
	// }

	// read an index
	private CompressedNode read(final int idxNum) throws IOException {
		MapEntry e = cache.get(idxNum);
		if (e == null) {
			Future<CompressedNode> future = service
					.submit(new Callable<CompressedNode>() {
						@Override
						public CompressedNode call() throws IOException {
							File f = getFile(idxNum);
							byte[] buffer = new byte[(int) f.length()];
							FileInputStream is = new FileInputStream(f);
							is.read(buffer);
							is.close();
							return new CompressedNode(buffer);
						}
					});
			// File f = getFile(idxNum);
			// byte[] buffer = new byte[(int) f.length()];
			// FileInputStream is = new FileInputStream(f);
			// is.read(buffer);
			// is.close();
			// CompressedNode cn = new CompressedNode(buffer);
			try {
				CompressedNode cn = future.get();
				e = new MapEntry(cn);
				cache.put(idxNum, e);
			} catch (InterruptedException e2) {
				throw new RuntimeException(e2);
			} catch (ExecutionException e2) {
				throw new RuntimeException(e2);
			}
		}
		return e.getNode();
	}

	public static class ListSet<T> extends ArrayList<T> {
		public ListSet() {
			super();
		}

		@Override
		public boolean add(T e) {
			if (!this.contains(e)) {
				return super.add(e);
			}
			return false;
		}
	}

	public class Writer implements Runnable {
		private CompressedNode cn;

		public Writer(CompressedNode cn) {
			this.cn = cn;
		}

		@Override
		public void run() {
			File f = getFile(cn.getIdx());
			// System.out.println("Writing " + f.getAbsolutePath());
			f.getParentFile().mkdirs();
			FileOutputStream os = null;
			try {
				os = new FileOutputStream(f);
				os.write(cn.getBuffer());
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				if (os != null) {
					IOUtils.closeQuietly(os);
				}
			}
		}
	}

	public class MapEntry {
		CompressedNode cn;
		Future<?> f;

		MapEntry(CompressedNode cn) {
			this.cn = cn;
			this.f = null;
		}

		MapEntry(CompressedNode cn, Future<?> f) {
			this.cn = cn;
			this.f = f;
		}

		public boolean done() {
			return f == null ? true : f.isDone();
		}

		public CompressedNode getNode() {
			return cn;
		}
	}
}
