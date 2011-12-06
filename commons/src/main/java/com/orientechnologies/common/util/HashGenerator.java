package com.orientechnologies.common.util;

/**
 * Port of hash code generation function from Bob Jenkins'
 * <a href"http://burtleburtle.net/bob/hash/doobs.html">package</a> .
 */
public abstract class HashGenerator {
  private static final long MAX_VALUE = 0xFFFFFFFFL;

  /**
   * hashlittle2: return 2 32-bit hash values
   * <p/>
   * This is good enough for hash table
   * lookup with 2^^64 buckets, or if you want a second hash if you're not
   * happy with the first, or if you want a probably-unique 64-bit ID for
   * the key.  first hash item is better mixed than second, so use it first.  If you want
   * a 64-bit value do something like "h[0] + h[1]<<32".
   *
   * @param key the key to hash
   * @param firstSeed  primary initialization value
   * @param secondSeed secondary initialization value
   * @return two 32-bit hash values
   */
  public static long[] hashtitle2(final byte[] key, final long firstSeed, final long secondSeed) {
    long a, b, c;
    long[] result = new long[2];

    a = b = c = add(add(0xdeadbeef, key.length), firstSeed);
    c = add(c, secondSeed);

    int length = key.length;
    int i = 0;
    while (length > 12) {
      a = add(a, byteToLong(key[i]));
      a = add(a, leftShift(byteToLong(key[i + 1]), 8));
      a = add(a, leftShift(byteToLong(key[i + 2]), 16));
      a = add(a, leftShift(byteToLong(key[i + 3]), 24));

      b = add(b, byteToLong(key[i + 4]));
      b = add(b, leftShift(byteToLong(key[i + 5]), 8));
      b = add(b, leftShift(byteToLong(key[i + 6]), 16));
      b = add(b, leftShift(byteToLong(key[i + 7]), 24));

      c = add(c, byteToLong(key[i + 8]));
      c = add(c, leftShift(byteToLong(key[i + 9]), 8));
      c = add(c, leftShift(byteToLong(key[i + 10]), 16));
      c = add(c, leftShift(byteToLong(key[i + 11]), 24));

      /*
      -------------------------------------------------------------------------------
      mix -- mix 3 32-bit values reversibly.

      This is reversible, so any information in (a,b,c) before mix() is
      still in (a,b,c) after mix().

      If four pairs of (a,b,c) inputs are run through mix(), or through
      mix() in reverse, there are at least 32 bits of the output that
      are sometimes the same for one pair and different for another pair.
      This was tested for:
      * pairs that differed by one bit, by two bits, in any combination
        of top bits of (a,b,c), or in any combination of bottom bits of
        (a,b,c).
      * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
        the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
        is commonly produced by subtraction) look like a single 1-bit
        difference.
      * the base values were pseudorandom, all zero but one bit set, or
        all zero plus a counter that starts at zero.

      Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
      satisfy this are
          4  6  8 16 19  4
          9 15  3 18 27 15
         14  9  3  7 17  3
      Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
      for "differ" defined as + with a one-bit base and a two-bit delta.  I
      used http://burtleburtle.net/bob/hash/avalanche.html to choose
      the operations, constants, and arrangements of the variables.

      This does not achieve avalanche.  There are input bits of (a,b,c)
      that fail to affect some output bits of (a,b,c), especially of a.  The
      most thoroughly mixed value is c, but it doesn't really even achieve
      avalanche in c.

      This allows some parallelism.  Read-after-writes are good at doubling
      the number of bits affected, so the goal of mixing pulls in the opposite
      direction as the goal of parallelism.  I did what I could.  Rotates
      seem to cost as much as shifts on every machine I could lay my hands
      on, and rotates are much kinder to the top and bottom bits, so I used
      rotates.
      -------------------------------------------------------------------------------
      */

      a = subtract(a, c);
      a = xor(a, rot(c, 4));
      c = add(c, b);
      b = subtract(b, a);
      b = xor(b, rot(a, 6));
      a = add(a, c);
      c = subtract(c, b);
      c = xor(c, rot(b, 8));
      b = add(b, a);
      a = subtract(a, c);
      a = xor(a, rot(c, 16));
      c = add(c, b);
      b = subtract(b, a);
      b = xor(b, rot(a, 19));
      a = add(a, c);
      c = subtract(c, b);
      c = xor(c, rot(b, 4));
      b = add(b, a);

      length -= 12;
      i += 12;
    }

    switch (length) {
      case 12:
        c = add(c, leftShift(byteToLong(key[i + 11]), 24));
      case 11:
        c = add(c, leftShift(byteToLong(key[i + 10]), 16));
      case 10:
        c = add(c, leftShift(byteToLong(key[i + 9]), 8));
      case 9:
        c = add(c, byteToLong(key[i + 8]));
      case 8:
        b = add(b, leftShift(byteToLong(key[i + 7]), 24));
      case 7:
        b = add(b, leftShift(byteToLong(key[i + 6]), 16));
      case 6:
        b = add(b, leftShift(byteToLong(key[i + 5]), 8));
      case 5:
        b = add(b, byteToLong(key[i + 4]));
      case 4:
        a = add(a, leftShift(byteToLong(key[i + 3]), 24));
      case 3:
        a = add(a, leftShift(byteToLong(key[i + 2]), 16));
      case 2:
        a = add(a, leftShift(byteToLong(key[i + 1]), 8));
      case 1:
        a = add(a, byteToLong(key[i]));
        break;
      case 0:
        result[0] = c;
        result[1] = b;
        return result;
    }

    /*
      -------------------------------------------------------------------------------
      final mixing of 3 32-bit values (a,b,c) into c

      Pairs of (a,b,c) values differing in only a few bits will usually
      produce values of c that look totally different.  This was tested for
      * pairs that differed by one bit, by two bits, in any combination
        of top bits of (a,b,c), or in any combination of bottom bits of
        (a,b,c).
      * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
        the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
        is commonly produced by subtraction) look like a single 1-bit
        difference.
      * the base values were pseudorandom, all zero but one bit set, or
        all zero plus a counter that starts at zero.

      These constants passed:
       14 11 25 16 4 14 24
       12 14 25 16 4 14 24
      and these came close:
       4  8 15 26 3 22 24
      10  8 15 26 3 22 24
      11  8 15 26 3 22 24
      -------------------------------------------------------------------------------
     */

    c = xor(c, b);
    c = subtract(c, rot(b, 14));
    a = xor(a, c);
    a = subtract(a, rot(c, 11));
    b = xor(b, a);
    b = subtract(b, rot(a, 25));
    c = xor(c, b);
    c = xor(c, rot(b, 16));
    a = xor(a, c);
    a = subtract(a, rot(c, 4));
    b = xor(b, a);
    b = subtract(b, rot(a, 14));
    c = xor(c, b);
    c = subtract(c, rot(b, 24));

    result[0] = c;
    result[1] = b;

    return result;
  }

  private static long rot(long x, long k) {
    return leftShift(x, k) | x >> (32 - k);
  }

  private static long add(final long a, final long b) {
    return (a + b) & MAX_VALUE;
  }

  private static long subtract(final long a, final long b) {
    return (a - b) & MAX_VALUE;
  }

  private static long leftShift(final long x, final long k) {
    return (x << k) & MAX_VALUE;
  }

  private static long xor(final long a, final long b) {
    return (a ^ b) & MAX_VALUE;
  }

  private static long byteToLong(final byte a) {
    return a & 0xFF;
  }
}
