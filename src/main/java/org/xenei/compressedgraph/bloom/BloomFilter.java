package org.xenei.compressedgraph.bloom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import org.xenei.compressedgraph.SerializableNode;
import org.xenei.compressedgraph.SerializableTriple;
import com.hp.hpl.jena.util.iterator.Filter;


public class BloomFilter extends Filter<SerializableTriple> {

    private final SerializableTriple target;
    private final boolean exact;
    private final BloomCapabilities io;
    private ByteBuffer targetBloomBuffer;
    private ByteBuffer candidateBloomBuffer;

    public BloomFilter(SerializableTriple target, BloomCapabilities io) {
      this.target = target;
      this.exact = !target.containsWild();
      this.io = io;
      this.targetBloomBuffer =  ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt( target.hashCode());
      this.candidateBloomBuffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    }

    // quick check of the bloom filter
    private boolean checkBloom(SerializableTriple candidate) {
      if (!io.supportsBloomQuery())
      {
        if (exact)
        {
          return target.hashCode() == candidate.hashCode();
        }
        else
        {
          BitSet targetSet = BitSet.valueOf( targetBloomBuffer );
          BitSet candidateSet = BitSet.valueOf( candidateBloomBuffer.putInt( 0, candidate.hashCode()) );
          targetSet.and( candidateSet );
          return targetSet.equals( candidateSet );
        }
      }
      if (!io.supportsExact())
      {
        return target.hashCode() == candidate.hashCode(); 
      }
      return true;
    }

    @Override
    public boolean accept(SerializableTriple candidate) {
      if (checkBloom(candidate)
          && match(target.getSubject(), candidate.getSubject())
          && match(target.getPredicate(), candidate.getPredicate())
          && match(target.getObject(), candidate.getObject())) {
        // make sure we can really return a triple before we accept it.
        try {
          candidate.getTriple();
        } catch (IOException e) {
          return false;
        }
        return true;
      }
      return false;
    }

    private boolean match(SerializableNode targetNode,
        SerializableNode answerNode) {
      return targetNode == null || targetNode == SerializableNode.ANY
          || targetNode.equals(answerNode);
    }

}
