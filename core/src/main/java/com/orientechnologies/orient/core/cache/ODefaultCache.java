/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class ODefaultCache implements OCache {
  int limit = 1000;

  private boolean enabled;

  OLinkedHashMapCache cache;
  OSharedResourceExternal lock = new OSharedResourceExternal();
  protected OMemoryWatchDog.Listener watchDogListener;

  ODefaultCache(int initialLimit) {
    if (initialLimit > 0)
      limit = initialLimit;
    cache = new OLinkedHashMapCache(limit, limit + 1, 0.75f);
  }

  public void startup() {
    watchDogListener = Orient.instance().getMemoryWatchDog().addListener(new LowMemoryListener());
  }

  public void shutdown() {
    try {
      lock.acquireExclusiveLock();
      cache.clear();
    } finally {
      lock.releaseExclusiveLock();
    }
    Orient.instance().getMemoryWatchDog().removeListener(watchDogListener);
    watchDogListener = null;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean enable() {
    final boolean enabledBefore = enabled;
    enabled = true;
    return !enabledBefore;
  }

  public boolean disable() {
    clear();
    final boolean enabledBefore = enabled;
    enabled = false;
    return enabledBefore;
  }

  public ORecordInternal<?> get(ORID id) {
    if (!enabled) return null;
    try {
      lock.acquireSharedLock();
      return cache.get(id);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public ORecordInternal<?> put(ORecordInternal<?> record) {
    if (!enabled) return null;
    try {
      lock.acquireExclusiveLock();
      return cache.put(record.getIdentity(), record);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public ORecordInternal<?> remove(ORID id) {
    if (!enabled) return null;
    try {
      lock.acquireExclusiveLock();
      return cache.remove(id);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void clear() {
    if (!enabled) return;
    try {
      lock.acquireExclusiveLock();
      cache.clear();
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int size() {
    try {
      lock.acquireSharedLock();
      return cache.size();
    } finally {
      lock.releaseSharedLock();
    }
  }

  public int limit() {
    try {
      lock.acquireSharedLock();
      return limit;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public Collection<ORID> keys() {
    try {
      lock.acquireSharedLock();
      Collection<ORID> keys = new ArrayList<ORID>(cache.size());
      keys.addAll(cache.keySet());
      return keys;
    } finally {
      lock.releaseSharedLock();
    }
  }

  private void removeEldest(int threshold) {
    try {
      lock.acquireExclusiveLock();
      cache.removeEldest(threshold);
    } finally {
      lock.releaseExclusiveLock();
    }
  }


  /**
   * Cache of records.
   *
   * @author Luca Garulli
   */
  @SuppressWarnings("serial")
  private class OLinkedHashMapCache extends LinkedHashMap<ORID, ORecordInternal<?>> {
    private int limit;

    public OLinkedHashMapCache(final int limit, final int initialCapacity, final float loadFactor) {
      super(initialCapacity, loadFactor, true);
      this.limit = limit;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<ORID, ORecordInternal<?>> iEldest) {
      final int size = size();
      if (limit == -1 || size < limit)
        // DON'T REMOVE ELDEST
        return false;

      if (limit - size > 1) {
        // REMOVE ITEMS MANUALLY
        removeEldest(limit - size);
        return false;
      } else
        return true;
    }

    public void removeEldest(final int threshold) {
      final ORID[] ridToRemove = new ORID[size() - threshold];

      int entryNum = 0;
      int i = 0;
      for (java.util.Map.Entry<ORID, ORecordInternal<?>> ridEntry : entrySet()) {
        if (!ridEntry.getValue().isDirty())
          if (entryNum++ >= threshold)
            // ADD ONLY AFTER THRESHOLD. THIS IS TO GET THE LESS USED
            ridToRemove[i++] = ridEntry.getKey();

        if (i >= ridToRemove.length)
          break;
      }

      for (ORID rid : ridToRemove)
        remove(rid);
    }
  }

  class LowMemoryListener implements OMemoryWatchDog.Listener {
    public void memoryUsageLow(final long freeMemory, final long freeMemoryPercentage) {
      try {
        if (freeMemoryPercentage < 10) {
          OLogManager.instance().debug(this, "Low memory (%d%%): clearing %d cached records", freeMemoryPercentage, size());
          clear();
        } else {
          final int oldSize = size();
          if (oldSize == 0)
            return;

          final int threshold = (int) (oldSize * 0.9f);
          ODefaultCache.this.removeEldest(threshold);
          OLogManager.instance().debug(this, "Low memory (%d%%): reducing cached records number from %d to %d",
            freeMemoryPercentage, oldSize, threshold);
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error occurred during default cache cleanup", e);
      }
    }

  }
}
