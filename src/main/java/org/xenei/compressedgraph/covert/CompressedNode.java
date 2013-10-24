package org.xenei.compressedgraph.covert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.xenei.compressedgraph.BitConstants;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.NodeVisitor;
import com.hp.hpl.jena.graph.Node_ANY;
import com.hp.hpl.jena.graph.Node_Blank;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

public class CompressedNode implements Serializable//, InvocationHandler 
{
	private static final char _ANON='A';
	private static final char _LIT='L';
	private static final char _LITC='l'; // compressed literal
	private static final char _URI='U';
	private static final char _VAR='V';
	private static final char _ANY = 'a';
	private static final int MAX_STR_SIZE = 20;
	private static final TypeMapper TYPE_MAPPER = new TypeMapper();
	
	private static final Method EQUALS;
	private static final Method HASH_CODE;
	private static final Method TO_STRING;
	private static final Method GET_IDX;
	
	public static final CompressedNode ANY;
	
	static {
		try {
			ANY = new CompressedNode( Node.ANY, BitConstants.WILD);
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException( e );
		}
		try {
			EQUALS = Object.class.getMethod("equals", Object.class);
			HASH_CODE = Object.class.getMethod("hashCode" );
			TO_STRING = Object.class.getMethod("toString" );
			GET_IDX = CompressedNode.class.getMethod("getIdx");
		}
		catch (NoSuchMethodException e)
		{
			throw new IllegalArgumentException( e );
		}

	}
	
	private transient Node node;
	private char type;
	private byte[] value;
	private int hashCode;
	private int idx;
	
//	public static CompressedNode newInstance( Node n, int idx ) throws IllegalArgumentException, IOException {
//		return (CompressedNode) Proxy.newProxyInstance(CompressedNode.class.getClassLoader(),
//                new Class<?>[] {Node.class, CompressedNode.class},
//                new CompressedNode(n, idx));
//	}
	
	private  CompressedNode( Node n, int idx ) throws IOException
	{
		this.idx = idx;
		this.hashCode = n.hashCode();
		this.node = n;
		
		if (n.equals( Node.ANY ))
		{
			type = _ANY;
			value = null;
		}
		else if (n.isVariable() ) {
			type=_VAR;
			value = encodeString( n.getName() );
		} else if (n.isURI()) {
			type=_URI;
			value = encodeString( n.getURI() );
		} else if (n.isBlank()) {
			type=_ANON;
			value = encodeString( n.getBlankNodeId().getLabelString());	
		} else if (n.isLiteral()) {
			type=_LIT;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			ObjectOutputStream oos = new ObjectOutputStream( baos );
			oos.writeUTF(n.getLiteralLexicalForm());
			oos.writeUTF(n.getLiteralLanguage() );
			oos.writeUTF(n.getLiteralDatatypeURI() );
			oos.close();
			value = baos.toByteArray();
			
			if (value.length > MAX_STR_SIZE )
			{
				type = _LITC;
				baos = new ByteArrayOutputStream();
				DeflaterOutputStream dos = new DeflaterOutputStream( baos );
				dos.write( value );
				dos.close();
				value = baos.toByteArray();
			}
		} else {
			throw new IllegalArgumentException( "Unknown node type "+n);
		}	
	}
	
	public int getIdx()
	{
		return idx;
	}
	
	public int hashCode()
	{
		return hashCode;
	}
	
	public boolean equals( Object o )
	{
		if (o instanceof CompressedNode )
		{
			CompressedNode cn = (CompressedNode)o;
			if (type==cn.type && value.length == cn.value.length)
			{
				for (int i=0;i<value.length;i++)
				{
					if (value[i] != cn.value[i])
					{
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}
	
	private Node getNode() throws IOException
	{
		if (node == null)
		{
			node = extractNode();
		}
		return node;
	}
	
	private String decodeString( byte[] b )
	{
		try {
			return new String( b, "UTF-8");
		}
		catch (UnsupportedEncodingException e)
		{	// should not happen
			throw new RuntimeException( e );
		} 
	}
	
	private Node extractNode() throws IOException
	{
		if (type == _ANON )
		{
			node = NodeFactory.createAnon( AnonId.create( decodeString( value )));
		}
		if (type == _LIT)
		{
			ByteArrayInputStream bais = new ByteArrayInputStream( value );

			ObjectInputStream ois = new ObjectInputStream( bais );
			String lex = ois.readUTF();
			String lang = ois.readUTF();
			String dtURI = ois.readUTF();
			ois.close();
			RDFDatatype dtype = TYPE_MAPPER.getSafeTypeByName(dtURI);
			node = NodeFactory.createLiteral(lex, lang, dtype);
		}
		if (type == _LITC)
		{
			ByteArrayInputStream bais = new ByteArrayInputStream( value );
			DeflaterInputStream dis = new DeflaterInputStream( bais );
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copy( dis, baos );
			dis.close();
			baos.close();
			bais = new ByteArrayInputStream( value );
			ObjectInputStream ois = new ObjectInputStream( bais );
			String lex = ois.readUTF();
			String lang = ois.readUTF();
			String dtURI = ois.readUTF();
			ois.close();
			RDFDatatype dtype = TYPE_MAPPER.getSafeTypeByName(dtURI);
			node = NodeFactory.createLiteral(lex, lang, dtype);
		}
		if (type == _URI)
		{
			node = NodeFactory.createURI( decodeString( value ));
		}
		if (type == _VAR)
		{
			node = NodeFactory.createVariable(decodeString( value ));
		}
		if (type == _ANY)
		{
			node = Node.ANY;
		}
		if (node == null)
		{
			throw new RuntimeException( "Unable to parse node");
		}
		return node;
	}
	
	private byte[] encodeString( String s )
	{
		try {
			return s.getBytes( "UTF-8" );
		}
		catch (UnsupportedEncodingException e)
		{	// should not happen
			throw new RuntimeException( e );
		}
	}
	
//	@Override
//	public Object invoke( final Object proxy, final Method method,
//			final Object[] args ) throws Throwable
//	{
//
//		// check for the special case methods
//		if (EQUALS.equals(method))
//		{
//			if (Proxy.isProxyClass(args[0].getClass()))
//			{
//				return args[0].equals(getNode());
//			}
//			else
//			{
//				return getNode().equals(args[0]);
//			}
//		}
//
//		if (HASH_CODE.equals(method))
//		{
//			return hashCode();
//		}
//
//		if (TO_STRING.equals(method))
//		{
//			return this.toString();
//		}
//		
//		if (GET_IDX.equals(method))
//		{
//			return this.getIdx();
//		}
//
//		// if we get here then the method is not being proxied so call the
//		// original method on the base item.
//		return method.invoke(getNode(), args);
//
//	}
}
