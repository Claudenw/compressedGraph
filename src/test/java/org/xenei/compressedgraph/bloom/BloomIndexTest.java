package org.xenei.compressedgraph.bloom;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public class BloomIndexTest {

	@Test
	public void testSingleByteIndex() {
		Byte[] expected = { (byte) 0xF1, (byte) 0xF3, (byte) 0xF5, (byte) 0xF7,
				(byte) 0xF9, (byte) 0xFB, (byte) 0xFD, (byte) 0xFF };
		List<Byte> lexp = Arrays.asList(expected);
		byte[] b = new byte[1];
		ByteBuffer bb = ByteBuffer.wrap(b);
		b[0] = (byte) 0xF1;
		List<ByteBuffer> lst = BloomIndex.getIndexIterator(bb).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i).get(0)));
		}

		b[0] = (byte) 0x1F;
		expected = new Byte[] { (byte) 0x1F, (byte) 0x3F, (byte) 0x5F,
				(byte) 0x7F, (byte) 0x9F, (byte) 0xBF, (byte) 0xDF, (byte) 0xFF };
		lexp = Arrays.asList(expected);
		lst = BloomIndex.getIndexIterator(bb).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i).get(0)));
		}
	}

	@Test
	public void testLimitIndex() {
		Byte[] expected = { (byte) 0xF1, (byte) 0xF3, (byte) 0xF5, (byte) 0xF7 };
		List<Byte> lexp = Arrays.asList(expected);
		byte[] b = new byte[1];
		ByteBuffer bb = ByteBuffer.wrap(b);
		b[0] = (byte) 0xF1;
		byte[] l = new byte[1];
		l[0] = (byte) 0xF8;

		List<ByteBuffer> lst = BloomIndex.getIndexIterator(bb,
				ByteBuffer.wrap(l)).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i).get(0)));
		}

		b[0] = (byte) 0x1F;
		l[0] = (byte) 0x7F;
		expected = new Byte[] { (byte) 0x1F, (byte) 0x3F, (byte) 0x5F,
				(byte) 0x7F, };
		lexp = Arrays.asList(expected);
		lst = BloomIndex.getIndexIterator(bb, ByteBuffer.wrap(l)).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i).get(0)));
		}
	}

	@Test
	public void testMultiByteIndex() {
		Integer[] expected = { 0xF1, 0xF3, 0xF5, 0xF7, 0xF9, 0xFB, 0xFD, 0xFF };

		Set<ByteBuffer> lexp = new TreeSet<ByteBuffer>(
				new BloomGraph.ByteBufferComparator());
		for (int i = 0; i < expected.length; i++) {
			byte[] buff = new byte[3];
			buff[0] = (byte) 0xFF;
			buff[1] = (byte) (int) expected[i];
			buff[2] = (byte) 0xFF;
			lexp.add(ByteBuffer.wrap(buff));
		}

		byte[] b = new byte[3];
		ByteBuffer bb = ByteBuffer.wrap(b);
		b[0] = (byte) 0xFF;
		b[1] = (byte) 0xF1;
		b[2] = (byte) 0xFF;
		List<ByteBuffer> lst = BloomIndex.getIndexIterator(bb).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i)));
		}
	}

	@Test
	public void testMultiByteLimitIndex() {
		Integer[] expected = { 0xF1, 0xF3, 0xF5, 0xF7 };

		Set<ByteBuffer> lexp = new TreeSet<ByteBuffer>(
				new BloomGraph.ByteBufferComparator());
		for (int i = 0; i < expected.length; i++) {
			byte[] buff = new byte[3];
			buff[0] = (byte) 0xFF;
			buff[1] = (byte) (int) expected[i];
			buff[2] = (byte) 0xFF;
			lexp.add(ByteBuffer.wrap(buff));
		}

		byte[] b = new byte[3];
		ByteBuffer bb = ByteBuffer.wrap(b);
		b[0] = (byte) 0xFF;
		b[1] = (byte) 0xF1;
		b[2] = (byte) 0xFF;

		byte[] l = new byte[3];
		l[0] = (byte) 0xFF;
		l[1] = (byte) 0xF8;
		l[2] = (byte) 0xFF;
		List<ByteBuffer> lst = BloomIndex.getIndexIterator(bb, ByteBuffer.wrap(l)).toList();
		assertEquals(expected.length, lst.size());
		for (int i = 0; i < lst.size(); i++) {
			assertTrue(String.format("missing %X", lst.get(i).get(0)),
					lexp.contains(lst.get(i)));
		}
	}
}
