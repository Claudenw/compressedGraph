package org.xenei.compressedgraph;

public interface NodeTypes {

	static final byte _ANON = 0x01;
	static final byte _LIT = 0x02;
	static final byte _URI = 0x03;
	static final byte _VAR = 0x04;
	static final byte _ANY = 0x05;
	static final byte _LITC = 0x12; // compressed literal
}
