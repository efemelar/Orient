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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Per database cache of documents.
 *
 * @author Luca Garulli
 * @author Sylvain Spinelli
 */
public class OLevel2RecordCache extends OAbstractRecordCache {

  private STRATEGY strategy;

  public enum STRATEGY {
    POP_RECORD, COPY_RECORD
  }

  public OLevel2RecordCache(final OStorage iStorage) {
    super("storage." + iStorage.getName(), OCacheLocator.secondaryCache());
    setStrategy(OGlobalConfiguration.CACHE_LEVEL2_STRATEGY.getValueAsInteger());
  }

  public void updateRecord(final ORecordInternal<?> fresh) {
    if (!isEnabled() || fresh == null || fresh.isDirty() || fresh.getIdentity().isNew())
      return;

    if (fresh.getIdentity().getClusterId() == excludedCluster)
      return;

    if (fresh.isPinned()) {
      final ORecordInternal<?> current = cache.get(fresh.getIdentity());
      if (current != null && current.getVersion() >= fresh.getVersion())
        return;

      if (databaseClosed(fresh)) {
        fresh.detach();
        cache.put(fresh);
      } else
        cache.put((ORecordInternal<?>) fresh.flatCopy());
    } else
      cache.remove(fresh.getIdentity());
  }

  private boolean databaseClosed(ORecordInternal<?> iRecord) {
    return !ODatabaseRecordThreadLocal.INSTANCE.isDefined() || iRecord.getDatabase().isClosed();
  }

  /**
   * Retrieve the record if any following the supported strategies: 0 = If found remove it (pop): the client (database instances)
   * will push it back when finished or on close. 1 = Return the instance but keep a copy in 2-level cache; this could help
   * highly-concurrent environment.
   *
   * @param iRID record identity
   * @return record if exists in cache, {@code null} otherwise
   */
  protected ORecordInternal<?> retrieveRecord(final ORID iRID) {
    if (!isEnabled() ||
      iRID.getClusterId() == excludedCluster)
      return null;

    final ORecordInternal<?> record = cache.remove(iRID);

    if (record == null || record.isDirty())
      return null;

    if (strategy == STRATEGY.COPY_RECORD)
      // PUT BACK A CLONE (THIS UPDATE ALSO THE LRU)
      cache.put((ORecordInternal<?>) record.flatCopy());

    return record;
  }

  public void setStrategy(final STRATEGY iStrategy) {
    strategy = iStrategy;
  }

  public void setStrategy(final int iStrategy) {
    strategy = STRATEGY.values()[iStrategy];
  }

  @Override
  public String toString() {
    return "STORAGE level2 cache records=" + getSize() + ", maxSize=" + getMaxSize();
  }
}
