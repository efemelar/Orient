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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.HashSet;
import java.util.Set;

/**
 * Cache of documents.
 *
 * @author Luca Garulli
 */
public abstract class OAbstractRecordCache extends OSharedResourceAbstract {
  protected OCache underlying;
  protected String profilerPrefix = "noname";
  protected int excludedCluster = -1;

  /**
   * Create the cache backed by given implementation
   *
   * @param cacheImpl actual implementation of cache
   */
  public OAbstractRecordCache(final OCache cacheImpl) {
    underlying = cacheImpl;
  }

  public boolean isEnabled() {
    return underlying.isEnabled();
  }

  public void setEnable(final boolean iValue) {
    if (iValue) underlying.enable();
    else underlying.disable();
  }

  public ORecordInternal<?> findRecord(final ORID iRid) {
    return null;
  }

  public ORecordInternal<?> freeRecord(final ORID iRID) {
    return underlying.remove(iRID);
  }

  public void freeCluster(final int clusterId) {
    final Set<ORID> toRemove = new HashSet<ORID>(underlying.size() / 2);

    for (ORID id : underlying.keys()) {
      if (id.getClusterId() == clusterId)
        toRemove.add(id);
    }
    for (ORID ridToRemove : toRemove)
      underlying.remove(ridToRemove);
  }

  /**
   * Delete a record entry from both database and storage caches.
   *
   * @param iRecord Record to remove
   */
  public void deleteRecord(final ORID iRecord) {
    underlying.remove(iRecord);
  }

  /**
   * Clear the entire cache by removing all the entries.
   */
  public void clear() {
    underlying.clear();
  }

  /**
   * Total number cached entries.
   *
   * @return number of cached entries
   */
  public int getSize() {
    return underlying.size();
  }

  public int getMaxSize() {
    return underlying.limit();
  }

  public void shutdown() {
    underlying.shutdown();
  }

  public void startup() {
    OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.enabled", new OProfilerHookValue() {
      public Object getValue() {
        return isEnabled();
      }
    });

    OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.current", new OProfilerHookValue() {
      public Object getValue() {
        return getSize();
      }
    });

    OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.max", new OProfilerHookValue() {
      public Object getValue() {
        return getMaxSize();
      }
    });
    underlying.startup();
  }

  public void setExcludedCluster(int excludedCluster) {
    this.excludedCluster = excludedCluster;
  }
}
