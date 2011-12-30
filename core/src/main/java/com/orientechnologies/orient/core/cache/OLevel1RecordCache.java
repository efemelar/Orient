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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

import static com.orientechnologies.orient.core.storage.OStorage.CLUSTER_INDEX_NAME;

/**
 * Per database cache of documents. It's not synchronized since database object are not thread-safes.
 *
 * @author Luca Garulli
 */
public class OLevel1RecordCache extends OAbstractRecordCache {
  private volatile OLevel2RecordCache secondary;
  private String PROFILER_CACHE_FOUND;
  private String PROFILER_CACHE_NOT_FOUND;

  public OLevel1RecordCache(ODatabaseRecord db) {
    super(new OCacheLocator().primaryCache());
  }

  @Override
  public void startup() {
    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    secondary = db.getLevel2Cache();

    profilerPrefix = "db." + db.getName();
    PROFILER_CACHE_FOUND = profilerPrefix + ".cache.found";
    PROFILER_CACHE_NOT_FOUND = profilerPrefix + ".cache.notFound";

    setExcludedCluster(db.getClusterIdByName(CLUSTER_INDEX_NAME));

    super.startup();
  }

  public void updateRecord(final ORecordInternal<?> iRecord) {
    if (!isEnabled() ||
      iRecord.getIdentity().getClusterId() == excludedCluster)
      return;

    if (underlying.get(iRecord.getIdentity()) != iRecord)
      underlying.put(iRecord);

    secondary.updateRecord(iRecord);
  }

  /**
   * Search a record in the cache and if found add it in the Database's level-1 cache.
   *
   * @param iRID RecordID to search
   * @return The record if found, otherwise null
   */
  public ORecordInternal<?> findRecord(final ORID iRID) {
    if (!isEnabled())
      return null;

    ORecordInternal<?> record = underlying.get(iRID);

    if (record == null) {
      record = secondary.retrieveRecord(iRID);

      if (record != null)
        underlying.put(record);
    }

    OProfiler.getInstance().updateCounter(record != null ? PROFILER_CACHE_FOUND : PROFILER_CACHE_NOT_FOUND, +1);

    return record;
  }

  /**
   * Delete a record entry from both database and storage caches.
   *
   * @param iRecord Record to remove
   */
  public void deleteRecord(final ORID iRecord) {
    super.deleteRecord(iRecord);
    secondary.freeRecord(iRecord);
  }

  public void shutdown() {
    super.shutdown();
    secondary = null;
  }

  @Override
  public void clear() {
    moveRecordsToSecondaryCache();
    super.clear();
  }

  public void moveRecordsToSecondaryCache() {
    if (secondary == null)
      return;

    for (ORID id : underlying.keys()) {
      secondary.updateRecord(underlying.get(id));
    }
  }

  public void invalidate() {
    underlying.clear();
  }

  @Override
  public String toString() {
    return "DB level1 cache records=" + getSize() + ", maxSize=" + getMaxSize();
  }
}
