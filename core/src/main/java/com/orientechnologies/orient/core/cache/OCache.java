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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.Collection;

/**
 *
 */
public interface OCache {
  /**
   * Contains all operations that should be run at cache initialization stage
   */
  void startup();

  /**
   * Contains all operations that should be run at cache destruction stage
   */
  void shutdown();

  /**
   * Tells whether cache is enabled
   *
   * @return true if cache enabled at call time, otherwise - false
   */
  boolean isEnabled();

  /**
   * Enables cache
   *
   * @return true - if enabled, false - otherwise (already enabled)
   */
  boolean enable();

  /**
   * Disables cache
   *
   * @return true - if disabled, false - otherwise (already disabled)
   */
  boolean disable();

  /**
   * Looks up for record in cache by it's identifier
   *
   * @param id unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  ORecordInternal<?> get(ORID id);

  /**
   * Pushes record to cache. Identifier of record used as access key
   *
   * @param record record that should be cached
   * @return previous version of record
   */
  ORecordInternal<?> put(ORecordInternal<?> record);

  /**
   * Removes record with specified identifier
   *
   * @param id unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  ORecordInternal<?> remove(ORID id);

  /**
   * Removes all records from cache
   */
  void clear();

  /**
   * Total number of stored records
   *
   * @return number of records in cache at the moment of call
   */
  int size();

  /**
   * Maximum number of items cache should keep
   *
   * @return number of records
   */
  int limit();

  /**
   * Keys of all stored in cache records
   *
   * @return keys of records
   */
  Collection<ORID> keys();
}
