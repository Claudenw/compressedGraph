package org.xenei.compressedgraph;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;
import org.xenei.compressedgraph.SerializableNode;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

public class SerializedNodeTest {

	private SerializableNode serializedNode;

	@Test
	public void testLongLiteral() throws Exception {
		Node n = NodeFactory
				.createLiteral("Autism is a disorder of neural development characterized by impaired social interaction and communication, and by restricted and repetitive behavior. The diagnostic criteria require that symptoms become apparent before a child is three years old. Autism affects information processing in the brain by altering how nerve cells and their synapses connect and organize; how this occurs is not well understood. It is one of three recognized disorders in the autism spectrum (ASDs), the other two being Asperger syndrome, which lacks delays in cognitive development and language, and pervasive developmental disorder, not otherwise specified (commonly abbreviated as PDD-NOS), which is diagnosed when the full set of criteria for autism or Asperger syndrome are not met. Autism has a strong genetic basis, although the genetics of autism are complex and it is unclear whether ASD is explained more by rare mutations, or by rare combinations of common genetic variants. In rare cases, autism is strongly associated with agents that cause birth defects. Controversies surround other proposed environmental causes, such as heavy metals, pesticides or childhood vaccines; the vaccine hypotheses are biologically implausible and lack convincing scientific evidence. The prevalence of autism is about 1\u20132 per 1,000 people worldwide, and the Centers for Disease Control and Prevention (CDC) report 20 per 1,000 children in the United States are diagnosed with ASD as of 2012 (up from 11 per 1000 in 2008). The number of people diagnosed with autism has been increasing dramatically since the 1980s, partly due to changes in diagnostic practice and government-subsidized financial incentives for named diagnoses; the question of whether actual prevalence has increased is unresolved. Parents usually notice signs in the first two years of their child's life. The signs usually develop gradually, but some autistic children first develop more normally and then regress. Early behavioral or cognitive intervention can help autistic children gain self-care, social, and communication skills. Although there is no known cure, there have been reported cases of children who recovered. Not many children with autism live independently after reaching adulthood, though some become successful. An autistic culture has developed, with some individuals seeking a cure and others believing autism should be accepted as a difference and not treated as a disorder.");

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);

		assertEquals(n.hashCode(), cn.hashCode());

		int i = cn.getSize();

		String s = n.getLiteralLexicalForm();
		int ii = s.getBytes("UTF-8").length;

		assertTrue(i <= ii);
	}

	private SerializableNode roundTrip(SerializableNode cn) throws IOException,
			ClassNotFoundException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(baos);
		os.writeObject(cn);
		os.close();
		ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(
				baos.toByteArray()));
		SerializableNode cn2 = (SerializableNode) is.readObject();

		assertEquals(cn.hashCode(), cn2.hashCode());
		assertEquals(cn.getType(), cn2.getType());
		assertEquals(cn.getSize(), cn2.getSize());
		assertArrayEquals(cn.getBuffer(), cn2.getBuffer());
		Node n1 = cn.getNode();
		Node n2 = cn2.getNode();

		assertEquals(n1, n2);
		return cn2;
	}

	@Test
	public void testLongLiteralSerialization() {

	}

	@Test
	public void testLongStringLiteralRoundTrip() throws Exception {
		Node n = NodeFactory
				.createLiteral("Autism is a disorder of neural development characterized by impaired social interaction and communication, and by restricted and repetitive behavior. The diagnostic criteria require that symptoms become apparent before a child is three years old. Autism affects information processing in the brain by altering how nerve cells and their synapses connect and organize; how this occurs is not well understood. It is one of three recognized disorders in the autism spectrum (ASDs), the other two being Asperger syndrome, which lacks delays in cognitive development and language, and pervasive developmental disorder, not otherwise specified (commonly abbreviated as PDD-NOS), which is diagnosed when the full set of criteria for autism or Asperger syndrome are not met. Autism has a strong genetic basis, although the genetics of autism are complex and it is unclear whether ASD is explained more by rare mutations, or by rare combinations of common genetic variants. In rare cases, autism is strongly associated with agents that cause birth defects. Controversies surround other proposed environmental causes, such as heavy metals, pesticides or childhood vaccines; the vaccine hypotheses are biologically implausible and lack convincing scientific evidence. The prevalence of autism is about 1\u20132 per 1,000 people worldwide, and the Centers for Disease Control and Prevention (CDC) report 20 per 1,000 children in the United States are diagnosed with ASD as of 2012 (up from 11 per 1000 in 2008). The number of people diagnosed with autism has been increasing dramatically since the 1980s, partly due to changes in diagnostic practice and government-subsidized financial incentives for named diagnoses; the question of whether actual prevalence has increased is unresolved. Parents usually notice signs in the first two years of their child's life. The signs usually develop gradually, but some autistic children first develop more normally and then regress. Early behavioral or cognitive intervention can help autistic children gain self-care, social, and communication skills. Although there is no known cure, there have been reported cases of children who recovered. Not many children with autism live independently after reaching adulthood, though some become successful. An autistic culture has developed, with some individuals seeking a cure and others believing autism should be accepted as a difference and not treated as a disorder.");

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testStringLiteralRoundTrip() throws Exception {
		Node n = NodeFactory.createLiteral("this is a short string.");

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testIntLiteralRoundTrip() throws Exception {
		LiteralLabel ll = LiteralLabelFactory.create(Integer.valueOf(5));
		Node n = NodeFactory.createLiteral(ll);

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testLongLiteralRoundTrip() throws Exception {
		LiteralLabel ll = LiteralLabelFactory.create(Long
				.valueOf(Long.MAX_VALUE));
		Node n = NodeFactory.createLiteral(ll);

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testDoubleLiteralRoundTrip() throws Exception {
		LiteralLabel ll = LiteralLabelFactory.create(Double.valueOf(3.14));
		Node n = NodeFactory.createLiteral(ll);

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testURIRoundTrip() throws Exception {

		Node n = NodeFactory.createURI("http://example.com/test");

		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testANYRoundTrip() throws Exception {
		SerializableNode cn = new SerializableNode(Node.ANY);
		SerializableNode cn2 = roundTrip(cn);

	}

	@Test
	public void testVariableRoundTrip() throws Exception {
		Node n = NodeFactory.createVariable("foo");
		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);
	}

	@Test
	public void testNamedAnonRoundTrip() throws Exception {
		Node n = NodeFactory.createAnon(AnonId.create("foo"));
		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);
	}

	@Test
	public void testAnonRoundTrip() throws Exception {
		Node n = NodeFactory.createAnon();
		SerializableNode cn = new SerializableNode(n);
		cn.setIdx(1);
		SerializableNode cn2 = roundTrip(cn);
	}
}
