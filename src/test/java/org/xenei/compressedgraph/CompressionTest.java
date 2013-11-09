package org.xenei.compressedgraph;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;

public class CompressionTest {

	@Test
	public void compTest() throws IOException {
		String s = "Autism is a disorder of neural development characterized by impaired social interaction and communication, and by restricted and repetitive behavior. The diagnostic criteria require that symptoms become apparent before a child is three years old. Autism affects information processing in the brain by altering how nerve cells and their synapses connect and organize; how this occurs is not well understood. It is one of three recognized disorders in the autism spectrum (ASDs), the other two being Asperger syndrome, which lacks delays in cognitive development and language, and pervasive developmental disorder, not otherwise specified (commonly abbreviated as PDD-NOS), which is diagnosed when the full set of criteria for autism or Asperger syndrome are not met. Autism has a strong genetic basis, although the genetics of autism are complex and it is unclear whether ASD is explained more by rare mutations, or by rare combinations of common genetic variants. In rare cases, autism is strongly associated with agents that cause birth defects. Controversies surround other proposed environmental causes, such as heavy metals, pesticides or childhood vaccines; the vaccine hypotheses are biologically implausible and lack convincing scientific evidence. The prevalence of autism is about 1\u20132 per 1,000 people worldwide, and the Centers for Disease Control and Prevention (CDC) report 20 per 1,000 children in the United States are diagnosed with ASD as of 2012 (up from 11 per 1000 in 2008). The number of people diagnosed with autism has been increasing dramatically since the 1980s, partly due to changes in diagnostic practice and government-subsidized financial incentives for named diagnoses; the question of whether actual prevalence has increased is unresolved. Parents usually notice signs in the first two years of their child's life. The signs usually develop gradually, but some autistic children first develop more normally and then regress. Early behavioral or cognitive intervention can help autistic children gain self-care, social, and communication skills. Although there is no known cure, there have been reported cases of children who recovered. Not many children with autism live independently after reaching adulthood, though some become successful. An autistic culture has developed, with some individuals seeking a cure and others believing autism should be accepted as a difference and not treated as a disorder.";

		byte[] v1 = s.getBytes("UTF-8");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream dos = new GZIPOutputStream(baos);
		dos.write(v1);
		dos.finish();
		dos.close();
		baos.close();
		byte[] v2 = baos.toByteArray();
		int i = v1.length;
		int ii = v2.length;
		ByteArrayInputStream bais = new ByteArrayInputStream(v2);
		GZIPInputStream dis = new GZIPInputStream(bais);
		BufferedInputStream bis = new BufferedInputStream(dis);
		byte[] v3 = new byte[v1.length];
		int start = 0;
		int end = i;
		while (end > 0) {
			int iii = bis.read(v3, start, end);
			start += iii;
			end -= iii;
		}

		for (int j = 0; j < i; j++) {
			assertEquals(v1[j], v3[j]);
		}
	}

	@Test
	public void testSerialization() {
		LiteralLabel ll1 = LiteralLabelFactory.create(Integer.valueOf(5));
		Node n = NodeFactory.createLiteral(ll1);
		String lex = n.getLiteralLexicalForm();
		String lang = n.getLiteralLanguage();
		String dtUri = n.getLiteralDatatypeURI();
		RDFDatatype dtype = TypeMapper.getInstance().getTypeByName(dtUri);

		LiteralLabel ll2 = LiteralLabelFactory.createLiteralLabel(lex, lang,
				dtype);
		assertEquals(ll1, ll2);
		Node n2 = NodeFactory.createLiteral(ll2);
		assertEquals(n, n2);
		n2 = NodeFactory.createLiteral(lex, null, dtype);
		assertEquals(n, n2);

	}
}
