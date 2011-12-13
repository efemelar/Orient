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
package com.orientechnologies.orient.core.metadata.schema;

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.orient.core.index.OIndex;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public interface OProperty extends Comparable<OProperty> {
	public static enum ATTRIBUTES {
		LINKEDTYPE, LINKEDCLASS, MIN, MAX, MANDATORY, NAME, NOTNULL, REGEXP, TYPE
	}

	public String getName();

	public OProperty setName(String iName);

	public OType getType();

	/**
	 * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded yet.
	 * 
	 * @return
	 */
	public OClass getLinkedClass();

	public OType getLinkedType();

	public boolean isNotNull();

	public OProperty setNotNull(boolean iNotNull);

	public boolean isMandatory();

	public OProperty setMandatory(boolean mandatory);

	public String getMin();

	public OProperty setMin(String min);

	public String getMax();

	public OProperty setMax(String max);

	/**
	 * Creates an index on this property. Indexes speed up queries but slow down insert and update operations. For massive inserts we
	 * suggest to remove the index, make the massive insert and recreate it.
	 * 
	 * 
	 * @param iType
	 *          One of types supported.
	 *          <ul>
	 *          <li>UNIQUE: Doesn't allow duplicates</li>
	 *          <li>NOTUNIQUE: Allow duplicates</li>
	 *          <li>FULLTEXT: Indexes single word for full text search</li>
	 *          </ul>
	 * @return see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)}.
	 */
	public OIndex<?> createIndex(final OClass.INDEX_TYPE iType);

	/**
	 * Remove the index on property
	 * 
	 * @return
	 * @deprecated Use {@link com.orientechnologies.orient.core.index.OIndexManager#dropIndex(String)} instead.
	 */
	@Deprecated
	public OPropertyImpl dropIndexes();

	/**
	 * @return All indexes in which this property participates as first key item.
	 * 
	 * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
	 */
	@Deprecated
	public Set<OIndex<?>> getIndexes();

	/**
	 * @return The first index in which this property participates as first key item.
	 * 
	 * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
	 */
	@Deprecated
	public Set<OIndex<?>> getIndex();

	/**
	 * @return All indexes in which this property participates.
	 */
	public Collection<OIndex<?>> getAllIndexes();

	/**
	 * Indicates whether property is contained in indexes as its first key item. If you would like to fetch all indexes or check
	 * property presence in other indexes use {@link #getAllIndexes()} instead.
	 * 
	 * @return <code>true</code> if and only if this property is contained in indexes as its first key item.
	 * @deprecated Use {@link OClass#areIndexed(String...)} instead.
	 */
	@Deprecated
	public boolean isIndexed();

	public String getRegexp();

	public OPropertyImpl setRegexp(String regexp);

	/**
	 * Change the type. It checks for compatibility between the change of type.
	 * 
	 * @param iType
	 */
	public OPropertyImpl setType(final OType iType);

	public Object get(ATTRIBUTES iAttribute);

	public void set(ATTRIBUTES attribute, Object iValue);
}
