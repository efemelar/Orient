/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.collection;

import com.orientechnologies.common.util.BitSet;
import com.orientechnologies.common.util.OMurmurHash3;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Random;

/**
 * Hash set implementation that is based on Cuckoo Hashing algorithm.
 * It is intended to effectively store integer numbers that is presented by int array.
 * It uses MurmurHash {@link com.orientechnologies.common.util.OMurmurHash3} for hash generation.
 */
public class OCuckooSet extends AbstractSet<byte[]> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final int BUCKET_SIZE = 4;
  private static final int MAX_TRIES = 1000;

  private int tableSeparator;
  private int maxTries;

  private byte[] cuckooTable;
  private BitSet bitSet;

  private int keySize;
  private int capacity;

  private int seedOne;
  private int seedTwo;

  public OCuckooSet(int initialCapacity,int keySize) {
    final int capacityBoundary = MAXIMUM_CAPACITY / (keySize * BUCKET_SIZE);

    if(initialCapacity > capacityBoundary)
      initialCapacity = capacityBoundary;

    capacity = 1;
    while (capacity < initialCapacity)
      capacity <<= 2;

    this.keySize = keySize;

    cuckooTable = new byte[keySize * capacity * BUCKET_SIZE];
    bitSet = new BitSet(cuckooTable.length);
    tableSeparator = cuckooTable.length >> 1;

    maxTries = Math.min(cuckooTable.length >> 1, MAX_TRIES);

    final Random random = new Random();
    seedOne = random.nextInt();
    seedTwo = random.nextInt();
  }

  @Override
  public boolean contains(Object o) {
    if (o.getClass() != byte[].class)
      return false;

    byte[] value = (byte[]) o;
    if (value.length != keySize)
      return false;

    final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, seedOne);

    final int beginIndexOne = indexOf(hashOne);
    if (checkBucket(value, beginIndexOne))
      return true;

    final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, seedTwo);
    final int beginIndexTwo = indexOf(hashTwo) + tableSeparator;

    return checkBucket(value, beginIndexTwo);
  }

  @Override
  public boolean add(byte[] bytes) {
    if(contains(bytes))
      return false;

    return false;
  }

  private boolean checkBucket(byte[] value, int beginIndex) {
    final int endIndex = beginIndex + BUCKET_SIZE;

    for(int i = beginIndex; i < endIndex; i++)
      for (int j = 0; j < keySize; j++) {
        if(!bitSet.get(i + j))
          return false;

        if (value[j] != cuckooTable[i + j])
          break;

        if (j == keySize - 1)
          return true;
      }
    return false;
  }

  private int indexOf(int hash) {
    return (hash & (capacity - 1)) * keySize * BUCKET_SIZE;
  }

  @Override
  public Iterator<byte[]> iterator() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }
}
