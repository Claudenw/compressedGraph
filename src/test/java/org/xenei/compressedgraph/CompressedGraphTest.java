/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.compressedgraph;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.xenei.compressedgraph.CompressedGraph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

public class CompressedGraphTest {

	private CompressedGraph graph = new CompressedGraph();
	
	@Test
	public void testAdd() {
		Node s = NodeFactory.createAnon();
		Node p = RDF.type.asNode();
		Node o = NodeFactory.createURI( "http://example.com/foo");
		graph.add( new Triple( s, p, o ));
		
		s = NodeFactory.createAnon();;
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/foz");
		o = NodeFactory.createLiteral( "A String");
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/fob");
		o = NodeFactory.createLiteral( "5" );
		graph.add( new Triple( s, p, o ));
		
		
	}
	
	@Test
	public void testFind() {
		Node s1 = NodeFactory.createAnon();
		Node p = RDF.type.asNode();
		Node o = NodeFactory.createURI( "http://example.com/foo");
		graph.add( new Triple( s1, p, o ));
		
		Node s = NodeFactory.createAnon();;
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/foz");
		o = NodeFactory.createLiteral( "A String");
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/fob");
		o = NodeFactory.createLiteral( "5" );
		graph.add( new Triple( s, p, o ));
		
		assertEquals( 4, graph.find( Node.ANY, Node.ANY, Node.ANY ).toList().size() );
		
		List<Triple> lst = graph.find( s1, Node.ANY, Node.ANY ).toList();
		assertEquals( 1, lst.size() );
		assertEquals( new Triple( s1, RDF.type.asNode(), NodeFactory.createURI( "http://example.com/foo")), lst.get(0) ); 
		
		lst = graph.find( Node.ANY, RDF.type.asNode(), Node.ANY ).toList();
		assertEquals( 2, lst.size() );
		assertTrue( lst.contains( new Triple( s1, RDF.type.asNode(), NodeFactory.createURI( "http://example.com/foo"))) ); 
		assertTrue( lst.contains( new Triple( s, RDF.type.asNode(), NodeFactory.createURI( "http://example.com/foo"))) ); 
		
		lst = graph.find( Node.ANY, Node.ANY, NodeFactory.createLiteral("5") ).toList();
		assertEquals( 1, lst.size() );
	}
	
	@Test
	public void testDelete() {
		Node s = NodeFactory.createAnon();
		Node p = RDF.type.asNode();
		Node o = NodeFactory.createURI( "http://example.com/foo");
		graph.add( new Triple( s, p, o ));
		
		s = NodeFactory.createAnon();;
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/foz");
		o = NodeFactory.createLiteral( "A String");
		graph.add( new Triple( s, p, o ));
		
		p = NodeFactory.createURI( "http://example.com/fob");
		o = NodeFactory.createLiteral( "5" );
		graph.add( new Triple( s, p, o ));
		
		List<Triple> t = graph.find( Node.ANY, Node.ANY, Node.ANY ).toList();
		assertEquals( 4, t.size());
		
		graph.delete( t.get(0) );
		List<Triple> t2 = graph.find( Node.ANY, Node.ANY, Node.ANY ).toList();
		assertEquals( 3, t2.size());
		
		assertTrue( t.containsAll(t2));
		assertFalse( t2.contains( t.get(0)));
		
	}
}
