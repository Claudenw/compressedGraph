package org.xenei.compressedgraph.bloom;

import java.nio.ByteBuffer;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;


public interface BloomCapabilities {
  
  /**
   * bloom values are correctly matched.
   * 
   * Bloom values are 4 bytes and may be interpreted as unsigned integers by the
   * underlying data store.
   * 
   * to support full bloom the underlying data store must match the bit patterns
   * as per the bloom filter definition  http://en.wikipedia.org/wiki/Bloom_filter
   * 
   * other support is to return all unsigned ints that are greater than or equal to
   * the requested 4 bytes interpreted as a little endian unsigned integer.
   * http://en.wikipedia.org/wiki/Endianness
   * 
   */
  boolean supportsFullBloom();
  
  /**
   * bloom filters may be matched exactly.
   * 
   * If the underlying data store will match the 4 byte bloom filter exactly
   * when the exact flag is set on the find method then this should return true.
   * 
   */
  boolean supportsExact();

  /**
   * Return true if a newly created graph is empty.
   * @return
   */
  public boolean canBeEmpty();
  /**
   * return false if the number of items in the store is not known or can only be
   * estimated.
   * @return true if the value returned by getSize() is an exact count of the 
   * number of items in the store.
   */
  public boolean sizeAccurate();
  /**
   * Reutrn the number of items in the store.  May be an estimate if sizeAccurate() 
   * returns false.  
   * @return the number of items in the store
   */
  public int getSize();
  
  /**
   * Return true if this store can be added to (not read only).
   */
  public boolean addAllowed();
  
  /**
   * return true if the data store will add duplicate blocks.
   */
  public boolean addsDuplicates();
  /**
   * Add an entry.
   * Systems must ensure that the data block only exists once in the data store.
   * In relational parlance the data could be considered as a primary key.
   * 
   * Write the entity to the store.
   * @param bloomValue The 4 byte bloom value used for searching
   * @param data the data to write.
   */
  void write(byte[] bloomValue, ByteBuffer data );
  
  /**
   * return true if this store premits deletion (not read only).
   */
  public boolean deleteAllowed();
  
  /**
   * Delete an entry.
   * Systems must ensure that only the specified data block is removed from the
   * store.  In relational parlance the data could be considered as a primary key.
   * 
   * @param bloomValue
   * @param data
   */
  void delete(byte[] bloomValue, ByteBuffer data);
  
  /**
   * Find a series of entries.
   * 
   * @param bloomValue the 4 byte bloom value to use for searching.
   * @param exact if true only exact matches should be returned (if supported).
   * @return An ExtendedIterator of the ByteBuffer values that were written to the store.
   */
  ExtendedIterator<ByteBuffer> find(byte[] bloomValue, boolean exact);
  
  /**
   * close the underlying connection to the data store
   */
  void close();
}
