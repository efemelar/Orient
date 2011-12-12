/*
 * Repackaged from org.apache.lucene.util.OpenBitSet (Lucene).
 * svn rev. 893130, http://svn.apache.org/repos/asf/lucene/java/trunk/
 *
 * Minor changes in class hierarchy, removed serialization.
 */


/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.util;


import java.util.Arrays;

/**
 * An "open" BitSet implementation that allows direct access to the array of words storing
 * the bits.
 * <p>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs is part
 * of the interface. This allows efficient implementation of other algorithms by someone
 * other than the author. It also allows one to efficiently implement alternate
 * serialization or interchange formats.<p/>
 * <p>
 *
 * The index range for a bitset can easily exceed positive <code>int</code> range in Java
 * (0x7fffffff), so many methods in this class accept or return a <code>long</code>.
 *
 * @author "Original implementation from the Lucene project."
 */
public class BitSet {
  /**
   * The initial default number of bits ({@value #DEFAULT_NUM_BITS}).
   */
  private static final long DEFAULT_NUM_BITS = 64;

  /**
   * Internal representation of bits in this bit set.
   */
  public long[] bits;

  /**
   * The number of words (longs) used in the {@link #bits} array.
   */
  public int wlen;


  /**
   * Constructs a bit set with the default capacity.
   */
  public BitSet() {
    this(DEFAULT_NUM_BITS);
  }


  /**
   * Constructs an BitSet large enough to hold numBits.
   */
  public BitSet(long numBits) {
    bits = new long[bits2words(numBits)];
    wlen = bits.length;
  }


  /**
   * Constructs an BitSet from an existing long[]. <br/>
   * The first 64 bits are in long[0], with bit index 0 at the least significant bit,
   * and bit index 63 at the most significant. Given a bit index, the word containing it
   * is long[index/64], and it is at bit number index%64 within that word.
   * <p/>
   * numWords are the number of elements in the array that contain set bits (non-zero
   * longs). numWords should be &lt;= bits.length, and any existing words in the array at
   * position &gt;= numWords should be zero.
   */
  public BitSet(long[] bits, int numWords) {
    this.bits = bits;
    this.wlen = numWords;
  }

  /**
   * Static constructor-like method similar to other (generic) collections.
   */
  public static BitSet newInstance() {
    return new BitSet();
  }


  /**
   * Returns the current capacity in bits (1 greater than the index of the last bit).
   */
  public long capacity() {
    return bits.length << 6;
  }


  /**
   * Returns the current capacity of this set. Included for compatibility. This is <b>not</b>
   * equal to {@link #cardinality}.
   *
   * @see #cardinality()
   * @see java.util.BitSet#size()
   */
  public long size() {
    return capacity();
  }

  /**
   * @see java.util.BitSet#length()
   */
  public long length() {
    trimTrailingZeros();
    if (wlen == 0) return 0;
    return (((long) wlen - 1) << 6)
            + (64 - Long.numberOfLeadingZeros(bits[wlen - 1]));
  }


  /**
   * Returns true if there are no set bits
   */
  public boolean isEmpty() {
    return cardinality() == 0;
  }


  /**
   * Returns true or false for the specified bit index.
   */
  public boolean get(int index) {
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.

    if (i >= bits.length) return false;
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index.
   */
  public boolean get(long index) {
    int i = (int) (index >> 6); // div 64
    if (i >= bits.length) return false;
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }


  /**
   * Sets a bit, expanding the set size if necessary.
   */
  public void set(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int) index & 0x3f;
    long bitmask = 1L << bit;
    bits[wordNum] |= bitmask;
  }


  /**
   * Sets a range of bits, expanding the set size if necessary
   *
   * @param startIndex lower index
   * @param endIndex   one-past the last bit to set
   */
  public void set(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;
    int startWord = (int) (startIndex >> 6);
    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = expandingWordNum(endIndex - 1);
    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
    // due to wrap
    if (startWord == endWord) {
      bits[startWord] |= (startmask & endmask);
      return;
    }

    bits[startWord] |= startmask;
    Arrays.fill(bits, startWord + 1, endWord, -1L);
    bits[endWord] |= endmask;
  }

  protected int expandingWordNum(long index) {
    int wordNum = (int) (index >> 6);
    if (wordNum >= wlen) {
      ensureCapacity(index + 1);
      wlen = wordNum + 1;
    }
    return wordNum;
  }

  /**
   * Clears all bits.
   */
  public void clear()
  {
    Arrays.fill(bits, 0);
    this.wlen = 0;
  }


  /**
   * clears a bit, allowing access beyond the current set size without changing the
   * <p/>
   * size.
   */

  public void clear(long index)
  {
    int wordNum = (int) (index >> 6); // div 64
    if (wordNum >= wlen) return;
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] &= ~bitmask;

  }


  /**
   * Clears a range of bits. Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex   one-past the last bit to clear
   */

  public void clear(int startIndex, int endIndex)
  {
    if (endIndex <= startIndex) return;
    int startWord = (startIndex >> 6);
    if (startWord >= wlen) return;


    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = ((endIndex - 1) >> 6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
    // due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;
    if (startWord == endWord)
    {
      bits[startWord] &= (startmask | endmask);
      return;
    }

    bits[startWord] &= startmask;
    int middle = Math.min(wlen, endWord);

    Arrays.fill(bits, startWord + 1, middle, 0L);
    if (endWord < wlen)
    {
      bits[endWord] &= endmask;
    }
  }


  /**
   * Clears a range of bits. Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex   one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex)
  {
    if (endIndex <= startIndex) return;

    int startWord = (int) (startIndex >> 6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = (int) ((endIndex - 1) >> 6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
    // due to wrap
    // invert masks since we are clearing

    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord)
    {
      bits[startWord] &= (startmask | endmask);

      return;
    }

    bits[startWord] &= startmask;
    int middle = Math.min(wlen, endWord);
    Arrays.fill(bits, startWord + 1, middle, 0L);

    if (endWord < wlen)
    {
      bits[endWord] &= endmask;
    }
  }


  /**
   * Sets a bit and returns the previous value. The index should be less than the BitSet
   * <p/>
   * size.
   */

  public boolean getAndSet(int index)

  {
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }


  /**
   * Sets a bit and returns the previous value. The index should be less than the BitSet
   * <p/>
   * size.
   */
  public boolean getAndSet(long index)
  {
    int wordNum = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }


  /**
   * Flips a bit, expanding the set size if necessary.
   */
  public void flip(long index)
  {
    int wordNum = expandingWordNum(index);
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
  }


  /**
   * flips a bit and returns the resulting bit value. The index should be less than the
   * <p/>
   * BitSet size.
   */

  public boolean flipAndGet(int index)
  {
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
    return (bits[wordNum] & bitmask) != 0;
  }


  /**
   * flips a bit and returns the resulting bit value. The index should be less than the
   * <p/>
   * BitSet size.
   */

  public boolean flipAndGet(long index)
  {
    int wordNum = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
    return (bits[wordNum] & bitmask) != 0;
  }


  /**
   * Flips a range of bits, expanding the set size if necessary
   *
   * @param startIndex lower index
   * @param endIndex   one-past the last bit to flip
   */

  public void flip(long startIndex, long endIndex)
  {
    if (endIndex <= startIndex) return;
    int startWord = (int) (startIndex >> 6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = expandingWordNum(endIndex - 1);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex

    // due to wrap
    if (startWord == endWord)
    {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;
    for (int i = startWord + 1; i < endWord; i++)
    {
      bits[i] = ~bits[i];
    }

    bits[endWord] ^= endmask;
  }


  /**
   * @return the number of set bits
   */
  public long cardinality()  {
    return BitUtil.pop_array(bits, 0, wlen);
  }


  /**
   * Returns the popcount or cardinality of the intersection of the two sets. Neither
   * <p/>
   * set is modified.
   */
  public static long intersectionCount(BitSet a, BitSet b)
  {
    return BitUtil.pop_intersect(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
  }


  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither set is
   * <p/>
   * modified.
   */
  public static long unionCount(BitSet a, BitSet b)  {
    long tot = BitUtil.pop_union(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen) {
      tot += BitUtil.pop_array(b.bits, a.wlen, b.wlen - a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen - b.wlen);
    }

    return tot;
  }


  /**
   * Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))".
   * <p/>
   * Neither set is modified.
   */
  public static long andNotCount(BitSet a, BitSet b)
  {
    long tot = BitUtil.pop_andnot(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen - b.wlen);
    }

    return tot;
  }


  /**
   * Returns the popcount or cardinality of the exclusive-or of the two sets. Neither
   * <p/>
   * set is modified.
   */

  public static long xorCount(BitSet a, BitSet b)  {
    long tot = BitUtil.pop_xor(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen)  {
      tot += BitUtil.pop_array(b.bits, a.wlen, b.wlen - a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen - b.wlen);
    }
    return tot;
  }


  /**
   * Returns the index of the first set bit starting at the index specified. -1 is
   * <p/>
   * returned if there are no more set bits.
   */

  public int nextSetBit(int index) {
    int i = index >> 6;
    if (i >= wlen) return -1;
    int subIndex = index & 0x3f; // index within the word
    long word = bits[i] >> subIndex; // skip all the bits to the right of index
    if (word != 0) {
      return (i << 6) + subIndex + BitUtil.ntz(word);
    }

    while (++i < wlen)  {
      word = bits[i];
      if (word != 0) return (i << 6) + BitUtil.ntz(word);
    }
    return -1;
  }


  /**
   * Returns the index of the first set bit starting at the index specified. -1 is
   * <p/>
   * returned if there are no more set bits.
   */

  public long nextSetBit(long index) {
    int i = (int) (index >>> 6);
    if (i >= wlen) return -1;
    int subIndex = (int) index & 0x3f; // index within the word
    long word = bits[i] >>> subIndex; // skip all the bits to the right of index
    if (word != 0) {
      return (((long) i) << 6) + (subIndex + BitUtil.ntz(word));
    }

    while (++i < wlen) {
      word = bits[i];
      if (word != 0) return (((long) i) << 6) + BitUtil.ntz(word);
    }


    return -1;
  }


  @Override

  public Object clone() {
    try {
      BitSet obs = (BitSet) super.clone();
      obs.bits = (long[]) obs.bits.clone(); // hopefully an array clone is as
      return obs;
    } catch (CloneNotSupportedException e)  {
      throw new RuntimeException(e);
    }
  }


  /**
   * this = this AND other
   */

  public void intersect(BitSet other) {
    int newLen = Math.min(this.wlen, other.wlen);
    long[] thisArr = this.bits;
    long[] otherArr = other.bits;

    // testing against zero can be more efficient

    int pos = newLen;
    while (--pos >= 0) {
      thisArr[pos] &= otherArr[pos];
    }

    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      Arrays.fill(bits, newLen, this.wlen, 0);
    }

    this.wlen = newLen;
  }

  /**
   * this = this OR other
   */
  public void union(BitSet other)  {
    int newLen = Math.max(wlen, other.wlen);
    ensureCapacityWords(newLen);

    long[] thisArr = this.bits;
    long[] otherArr = other.bits;

    int pos = Math.min(wlen, other.wlen);

    while (--pos >= 0) {
      thisArr[pos] |= otherArr[pos];
    }

    if (this.wlen < newLen) {
      System.arraycopy(otherArr, this.wlen, thisArr, this.wlen, newLen - this.wlen);
    }

    this.wlen = newLen;
  }


  /**
   * Remove all elements set in other. this = this AND_NOT other
   */
  public void remove(BitSet other) {
    int idx = Math.min(wlen, other.wlen);
    long[] thisArr = this.bits;

    long[] otherArr = other.bits;

    while (--idx >= 0) {
      thisArr[idx] &= ~otherArr[idx];
    }
  }


  /**
   * this = this XOR other
   */

  public void xor(BitSet other) {
    int newLen = Math.max(wlen, other.wlen);

    ensureCapacityWords(newLen);

    long[] thisArr = this.bits;
    long[] otherArr = other.bits;

    int pos = Math.min(wlen, other.wlen);

    while (--pos >= 0)
    {
      thisArr[pos] ^= otherArr[pos];
    }

    if (this.wlen < newLen) {
      System.arraycopy(otherArr, this.wlen, thisArr, this.wlen, newLen - this.wlen);
    }

    this.wlen = newLen;
  }

  // some BitSet compatibility methods
  // ** see {@link intersect} */
  public void and(BitSet other) {
    intersect(other);
  }


  // ** see {@link union} */
  public void or(BitSet other) {
    union(other);
  }


  // ** see {@link andNot} */
  public void andNot(BitSet other) {
    remove(other);
  }


  /**
   * returns true if the sets have any elements in common
   */
  public boolean intersects(BitSet other) {
    int pos = Math.min(this.wlen, other.wlen);
    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    while (--pos >= 0) {
      if ((thisArr[pos] & otherArr[pos]) != 0) return true;
    }

    return false;
  }


  /**
   * Expand the long[] with the size given as a number of words (64 bit longs).
   * <p/>
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacityWords(int numWords) {
    if (bits.length < numWords) {
      bits = grow(bits, numWords);
    }
  }


  public static long[] grow(long[] array, int minSize) {
    if (array.length < minSize) {
     long[] newArray = new long[getNextSize(minSize)];
     System.arraycopy(array, 0, newArray, 0, array.length);
      return newArray;
    } else return array;
  }


  public static int getNextSize(int targetSize)
  {
    /*
    * This over-allocates proportional to the list size, making room for additional
    * growth. The over-allocation is mild, but is enough to give linear-time
    * amortized behavior over a long sequence of appends() in the presence of a
    * poorly-performing system realloc(). The growth pattern is: 0, 4, 8, 16, 25, 35,
    * 46, 58, 72, 88, ...
    */
    return (targetSize >> 3) + (targetSize < 9 ? 3 : 6) + targetSize;
  }


  /**
   * Ensure that the long[] is big enough to hold numBits, expanding it if necessary.
   * <p/>
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacity(long numBits)  {
    ensureCapacityWords(bits2words(numBits));
  }


  /**
   * Lowers {@link #wlen}, the number of words in use, by checking for trailing zero
   * <p/>
   * words.
   */
  public void trimTrailingZeros() {
    int idx = wlen - 1;
    while (idx >= 0 && bits[idx] == 0)
      idx--;
    wlen = idx + 1;
  }


  /**
   * returns the number of 64 bit words it would take to hold numBits
   */
  public static int bits2words(long numBits) {
    return (int) (((numBits - 1) >>> 6) + 1);
  }


  /**
   * returns true if both sets have the same bits set
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BitSet)) return false;
    BitSet a;
    BitSet b = (BitSet) o;
    // make a the larger set.

    if (b.wlen > this.wlen) {
      a = b;
      b = this;
    } else {
      a = this;
    }

    // check for any set bits out of the range of b
    for (int i = a.wlen - 1; i >= b.wlen; i--)
    {
      if (a.bits[i] != 0) return false;
    }


    for (int i = b.wlen - 1; i >= 0; i--)
    {
      if (a.bits[i] != b.bits[i]) return false;
    }

    return true;
  }


  @Override

  public int hashCode()  {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = bits.length; --i >= 0; ) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 63); // rotate left
    }


    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h >> 32) ^ h) + 0x98761234;
  }


  @Override

  public String toString()

  {
    long bit = nextSetBit(0);
    if (bit < 0) {
      return "{}";
    }


    final StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append(Long.toString(bit));
    while ((bit = nextSetBit(bit + 1)) >= 0)
    {
      builder.append(", ");
      builder.append(Long.toString(bit));
    }

    builder.append("}");
    return builder.toString();
  }
}
