package org.xenei.compressedgraph.covert;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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

public class CompressedNode implements Serializable
{
	private static final char _ANON='A';
	private static final char _LIT='L';
	private static final char _LITC='l'; // compressed literal
	private static final char _URI='U';
	private static final char _VAR='V';
	private static final char _ANY = 'a';
	private static final int MAX_STR_SIZE = 20;
	private static final TypeMapper TYPE_MAPPER = new TypeMapper();
	
	public static final CompressedNode ANY;
	
	static {
		try {
			ANY = new CompressedNode( Node.ANY, BitConstants.WILD);
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException( e );
		}
	}
	
	private transient SoftReference<Node> node;
	private char type;
	private byte[] value;
	private int hashCode;
	private int idx;
		
	public  CompressedNode( Node n, int idx ) throws IOException
	{
		this.idx = idx;
		this.hashCode = n.hashCode();
		this.node = new SoftReference<Node>( n );
		
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
			DataOutputStream os = new DataOutputStream( baos );
			write(os, n.getLiteralLexicalForm());
			write(os, n.getLiteralLanguage());
			write(os, n.getLiteralDatatypeURI());
			os.close();
			baos.close();
			value = baos.toByteArray();
			
			if (value.length > MAX_STR_SIZE )
			{
				type = _LITC;
				baos = new ByteArrayOutputStream();
				GZIPOutputStream dos = new GZIPOutputStream( baos );
				dos.write( value );
				dos.close();
				value = baos.toByteArray();
			}
		} else {
			throw new IllegalArgumentException( "Unknown node type "+n);
		}	
	}
	
	private void write(DataOutputStream os, String s ) throws IOException
	{
		if (s == null )
		{
			os.writeInt( 0 );
		}
		else
		{
			byte[] b = encodeString( s);
			os.writeInt( b.length );
			if (b.length > 0)
			{
				os.write( b );
			}
		}
	}
	
	public void setIdx( int idx )
	{
		if (this.idx != BitConstants.WILD)
		{
			throw new IllegalStateException( "CompressedNode id must be WILD to be set" );
		}
		this.idx = idx;
	}
	
	public int getSize()
	{
		return value.length;
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
	
	public Node getNode() throws IOException
	{
		if (node == null || node.get() == null)
		{
			extractNode();
		}
		return node.get();
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
	
	private String read(DataInputStream is) throws IOException
	{
		int n = is.readInt();
		if (n == 0)
		{
			return null;
		}
		byte[] b = new byte[n];
		is.read(b);
		return decodeString( b );
	}
	
	private void extractNode() throws IOException
	{
		if (type == _ANON )
		{
			node = new SoftReference<Node>( NodeFactory.createAnon( AnonId.create( decodeString( value ))));
		}
		if (type == _LIT || type == _LITC)
		{
			
			InputStream bais = new ByteArrayInputStream( value );
			if (type == _LITC)
			{
				bais = new GZIPInputStream( bais );	
			}
			DataInputStream is = new DataInputStream( new BufferedInputStream( bais ) );
			String lex = read(is);
			String lang = read(is);;
			String dtURI = read(is);
			is.close();
			RDFDatatype dtype = StringUtils.isEmpty(dtURI)?null:TYPE_MAPPER.getSafeTypeByName(dtURI);
			node = new SoftReference<Node>( NodeFactory.createUncachedLiteral(lex, lang, dtype));
		}
		if (type == _URI)
		{
			node = new SoftReference<Node>( NodeFactory.createURI( decodeString( value )));
		}
		if (type == _VAR)
		{
			node = new SoftReference<Node>( NodeFactory.createVariable(decodeString( value )));
		}
		if (type == _ANY)
		{
			node = new SoftReference<Node>( Node.ANY );
		}
		if (node == null)
		{
			throw new RuntimeException( "Unable to parse node");
		}
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
}
