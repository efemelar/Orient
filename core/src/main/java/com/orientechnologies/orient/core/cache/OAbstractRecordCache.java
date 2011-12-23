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
  protected OCache cache;
  protected String profilerPrefix;
  protected int excludedCluster = -1;

  /**
   * Create the cache of iMaxSize size.
   *
   * @param iProfilerPrefix prefix used to distinguish cache profiler from others
   * @param cacheImpl       actual implementation of cache
   */
  public OAbstractRecordCache(final String iProfilerPrefix, final OCache cacheImpl) {
    profilerPrefix = iProfilerPrefix;
    cache = cacheImpl;
  }

  public boolean isEnabled() {
    return cache.isEnabled();
  }

  public void setEnable(final boolean iValue) {
    if (iValue) cache.enable();
    else cache.disable();
  }

  public ORecordInternal<?> findRecord(final ORID iRid) {
    return null;
  }

  public ORecordInternal<?> freeRecord(final ORID iRID) {
    return cache.remove(iRID);
  }

  public void freeCluster(final int clusterId) {
    final Set<ORID> toRemove = new HashSet<ORID>(cache.size() / 2);

    for (ORID id : cache.keys()) {
      if (id.getClusterId() == clusterId)
        toRemove.add(id);
    }
    for (ORID ridToRemove : toRemove)
      cache.remove(ridToRemove);
  }

  /**
   * Delete a record entry from both database and storage caches.
   *
   * @param iRecord Record to remove
   */
  public void deleteRecord(final ORID iRecord) {
    if (!cache.isEnabled())
      return;
    cache.remove(iRecord);
  }

  /**
   * Clear the entire cache by removing all the entries.
   */
  public void clear() {
    cache.clear();
  }

  /**
   * Total number cached entries.
   *
   * @return number of cached entries
   */
  public int getSize() {
    return cache.size();
  }

  public int getMaxSize() {
    return cache.limit();
  }

  public void shutdown() {
    cache.shutdown();
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
  }

  public void setExcludedCluster(int excludedCluster) {
    this.excludedCluster = excludedCluster;
  }
}
