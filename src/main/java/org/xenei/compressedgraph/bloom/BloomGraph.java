package org.xenei.compressedgraph.bloom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;

import org.xenei.compressedgraph.SerializableTriple;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;


public class BloomGraph extends GraphBase {
  
  private final BloomCapabilities io;
  
   public BloomGraph( BloomCapabilities io )
   {
     this.io = io;
   }

  
  @Override
  protected final ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {

    SerializableTriple target;
    try {
      target = new SerializableTriple(m.asTriple());
    } catch (IOException e) {
      return WrappedIterator.emptyIterator();
    }

    boolean exact = !target.containsWild();
    return WrappedIterator.createNoRemove(io.find(getBloomValue(target), exact))
        .mapWith(new Map1<ByteBuffer,SerializableTriple>(){

          public SerializableTriple map1(ByteBuffer o) {
            return new SerializableTriple( o );
          }})
        .filterKeep(new BloomFilter(target, io))
        .mapWith(new Map1<SerializableTriple, Triple>() {

          public Triple map1(SerializableTriple o) {
            try {
              return o.getTriple();
            } catch (IOException e) {
              throw new RuntimeException("Triple not created during iteration");
            }
          }
        });

  }

  private byte[] getBloomValue( SerializableTriple t )
  {
    byte[] bloom = new byte[4];
    ByteBuffer bb = ByteBuffer.wrap( bloom ).order(ByteOrder.BIG_ENDIAN).putInt(0,t.hashCode());
    return bloom;
  }
  
  public static Long getBloomValue( byte[] bloom )
  {
	  byte[] b = new byte[8];
	  ByteBuffer buff = ByteBuffer.wrap( b ).order(ByteOrder.BIG_ENDIAN);
	  buff.position( 4 );
	  buff.put( bloom );
	  return Long.valueOf(buff.getLong(0));
  }
  
  @Override
  public final void performAdd(Triple t) {
    SerializableTriple st;
    
    try {
      st = new SerializableTriple(t);
      boolean dup = false;
      if (io.addsDuplicates())
      {
    	  dup = io.find(getBloomValue(st), true).filterKeep( new ExactMatchFilter( st )).hasNext();
      }
      if (!dup)
      {
    	  io.write( getBloomValue(st), st.getByteBuffer() );
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void performDelete(Triple t) {
    SerializableTriple target;
    try {
      target = new SerializableTriple(t);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    io.delete( getBloomValue(target), target.getByteBuffer() );
  }
  
  @Override
  public void close() {
    io.close();
    super.close();
  }
  
  @Override
  protected int graphBaseSize() {
    return io.getSize();
  }

  @Override
  public Capabilities getCapabilities() {
    if (capabilities == null) {
      capabilities = new MyCapabilities(io);
    }
    return capabilities;
  }

  public static class ExactMatchFilter extends Filter<ByteBuffer>
  {
	  private SerializableTriple t;
	  private ByteBufferComparator comp;
	  
	  ExactMatchFilter(SerializableTriple t)
	  {
		  this.t = t;
		  comp = new ByteBufferComparator();
	  }
	@Override
	public boolean accept(ByteBuffer o) {
		return comp.compare( t.getByteBuffer(), o) == 0;
	}
	
  }
  
  public static class ByteBufferComparator implements Comparator<ByteBuffer> {

		@Override
		public int compare(ByteBuffer arg0, ByteBuffer arg1) {
			int len = Math.min( arg0.capacity(), arg1.capacity());
			arg0.position(0);
			arg1.position(0);
			int retval;
			for (int i=0;i<len;i++)
			{
				byte b0 = arg0.get();
				byte b1 = arg1.get();
				retval = (b0<b1)?-1:(b0==b1)?0:1;
				if (retval != 0)
				{
					return retval;
				}
			}
			return arg0.capacity() < arg1.capacity()?-1:(arg0.capacity() == arg1.capacity())?0:1;
		}	
	};
	
  private class MyCapabilities implements Capabilities {
    
    private BloomCapabilities io;
    
    MyCapabilities( BloomCapabilities io )
    {
      this.io = io;
    }

    public boolean sizeAccurate() {
      return io.sizeAccurate();
    }

    public boolean addAllowed() {
      return io.addAllowed();
    }

    public boolean addAllowed(boolean every) {
      return addAllowed();
    }

    public boolean deleteAllowed() {
      return io.deleteAllowed();
    }

    public boolean deleteAllowed(boolean every) {
      return deleteAllowed();
    }

    public boolean canBeEmpty() {
      return io.canBeEmpty();
    }

    public boolean iteratorRemoveAllowed() {
      return false;
    }

    public boolean findContractSafe() {
      return true;
    }

    public boolean handlesLiteralTyping() {
      return false;
    }
  }
}
