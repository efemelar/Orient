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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyListIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Schema Class implementation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OClassImpl extends ODocumentWrapperNoClass implements OClass {

	protected OSchemaShared									owner;
	protected String												name;
	protected Class<?>											javaClass;
	protected int														fixedSize		= 0;
	protected final Map<String, OProperty>	properties	= new HashMap<String, OProperty>();

	protected int[]													clusterIds;
	protected int														defaultClusterId;
	protected OClassImpl										superClass;
	protected int[]													polymorphicClusterIds;
	protected List<OClass>									baseClasses;
	protected float													overSize		= 0f;
	protected String												shortName;

	/**
	 * Constructor used in unmarshalling.
	 */
	public OClassImpl() {
	}

	/**
	 * Constructor used in unmarshalling.
	 */
	protected OClassImpl(final OSchemaShared iOwner) {
		document = new ODocument(getDatabase());
		owner = iOwner;
	}

	/**
	 * Constructor used in unmarshalling.
	 */
	protected OClassImpl(final OSchemaShared iOwner, final ODocument iDocument) {
		document = iDocument;
		owner = iOwner;
	}

	protected OClassImpl(final OSchemaShared iOwner, final String iName, final String iJavaClassName, final int[] iClusterIds)
			throws ClassNotFoundException {
		this(iOwner, iName, iClusterIds);
		javaClass = Class.forName(iJavaClassName);
	}

	protected OClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
		this(iOwner);
		name = iName;
		setClusterIds(iClusterIds);
		setPolymorphicClusterIds(iClusterIds);
		defaultClusterId = iClusterIds[0];
	}

	public <T> T newInstance() throws InstantiationException, IllegalAccessException {
		if (javaClass == null)
			throw new IllegalArgumentException("Cannot create an instance of class '" + name + "' since no Java class was specified");

		return (T) javaClass.newInstance();
	}

	public void validateInstances() {
		ODatabaseComplex<?> current = getDatabase().getDatabaseOwner();

		while (current != null && current.getUnderlying() instanceof ODatabaseComplex<?> && !(current instanceof ODatabaseDocumentTx))
			current = current.getUnderlying();

		for (ODocument d : ((ODatabaseDocumentTx) current).browseClass(name, true)) {
			d.validate();
		}
	}

	public OClass getSuperClass() {
		return superClass;
	}

	/**
	 * Set the super class.
	 * 
	 * @param iSuperClass
	 *          Super class as OClass instance
	 * @return the object itself.
	 */
	public OClass setSuperClass(final OClass iSuperClass) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		final String cmd = String.format("alter class %s superclass %s", name, iSuperClass.getName());
		getDatabase().command(new OCommandSQL(cmd)).execute();
		setSuperClassInternal(iSuperClass);
		return this;
	}

	public void setSuperClassInternal(final OClass iSuperClass) {
		this.superClass = (OClassImpl) iSuperClass;
		superClass.addBaseClasses(this);
	}

	public String getName() {
		return name;
	}

	public OClass setName(final String iName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		final String cmd = String.format("alter class %s name %s", name, iName);
		getDatabase().command(new OCommandSQL(cmd)).execute();
		name = iName;
		return this;
	}

	public void setNameInternal(final String iName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		owner.changeClassName(name, iName);
		name = iName;
	}

	public String getShortName() {
		return shortName;
	}

	public OClass setShortName(final String iShortName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		final String cmd = String.format("alter class %s shortname %s", name, iShortName);
		getDatabase().command(new OCommandSQL(cmd)).execute();
		setShortNameInternal(iShortName);
		return this;
	}

	public void setShortNameInternal(final String shortName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		if (this.shortName != null)
			// UNREGISTER ANY PREVIOUS SHORT NAME
			owner.classes.remove(shortName);

		this.shortName = shortName;

		// REGISTER IT
		owner.classes.put(shortName.toLowerCase(), this);
	}

	public String getStreamableName() {
		return shortName != null ? shortName : name;
	}

	public Collection<OProperty> declaredProperties() {
		return Collections.unmodifiableCollection(properties.values());
	}

	public Collection<OProperty> properties() {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

		Collection<OProperty> props = null;

		OClassImpl currentClass = this;

		do {
			if (currentClass.properties != null) {
				if (props == null)
					props = new ArrayList<OProperty>();
				props.addAll(currentClass.properties.values());
			}

			currentClass = (OClassImpl) currentClass.getSuperClass();

		} while (currentClass != null);

		return (Collection<OProperty>) (props != null ? props : Collections.emptyList());
	}

	public Collection<OProperty> getIndexedProperties() {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

		Collection<OProperty> indexedProps = null;

		OClassImpl currentClass = this;

		do {
			if (currentClass.properties != null) {
				for (OProperty p : currentClass.properties.values())
					if (areIndexed(p.getName())) {
						if (indexedProps == null)
							indexedProps = new ArrayList<OProperty>();
						indexedProps.add(p);
					}
			}

			currentClass = (OClassImpl) currentClass.getSuperClass();

		} while (currentClass != null);

		return (Collection<OProperty>) (indexedProps != null ? indexedProps : Collections.emptyList());
	}

	public OProperty getProperty(final String iPropertyName) {
		OClassImpl currentClass = this;
		OProperty p = null;

		do {
			if (currentClass.properties != null)
				p = currentClass.properties.get(iPropertyName.toLowerCase());

			if (p != null)
				return p;

			currentClass = (OClassImpl) currentClass.getSuperClass();

		} while (currentClass != null);

		return p;
	}

	public OProperty createProperty(final String iPropertyName, final OType iType) {
		return addProperty(iPropertyName, iType, null, null);
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
		if (iLinkedClass == null)
			throw new OSchemaException("Missing linked class");

		return addProperty(iPropertyName, iType, null, iLinkedClass);
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
		return addProperty(iPropertyName, iType, iLinkedType, null);
	}

	public int fixedSize() {
		return fixedSize;
	}

	public boolean existsProperty(final String iPropertyName) {
		return properties.containsKey(iPropertyName.toLowerCase());
	}

	public void dropProperty(final String iPropertyName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

		final String lowerName = iPropertyName.toLowerCase();

		if (!properties.containsKey(lowerName))
			throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");

		final StringBuilder cmd = new StringBuilder("drop property ");
		// CLASS.PROPERTY NAME
		cmd.append(name);
		cmd.append('.');
		cmd.append(iPropertyName);

		getDatabase().command(new OCommandSQL(cmd.toString())).execute();

		if (existsProperty(iPropertyName))
			properties.remove(lowerName);
	}

	public void dropPropertyInternal(final String iPropertyName) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

		final OProperty prop = properties.remove(iPropertyName.toLowerCase());

		if (prop == null)
			throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");

		fixedSize -= prop.getType().size;
	}

	public OProperty addProperty(final String iPropertyName, final OType iType, final OType iLinkedType, final OClass iLinkedClass) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		final String lowerName = iPropertyName.toLowerCase();

		if (properties.containsKey(lowerName))
			throw new OSchemaException("Class " + name + " already has property '" + iPropertyName + "'");

		final StringBuilder cmd = new StringBuilder("create property ");
		// CLASS.PROPERTY NAME
		cmd.append(name);
		cmd.append('.');
		cmd.append(iPropertyName);

		// TYPE
		cmd.append(' ');
		cmd.append(iType.name);

		if (iLinkedType != null) {
			// TYPE
			cmd.append(' ');
			cmd.append(iLinkedType.name);

		} else if (iLinkedClass != null) {
			// TYPE
			cmd.append(' ');
			cmd.append(iLinkedClass.getName());
		}

		getDatabase().command(new OCommandSQL(cmd.toString())).execute();

		if (existsProperty(iPropertyName))
			return properties.get(lowerName);
		else
			// ADD IT LOCALLY AVOIDING TO RELOAD THE ENTIRE SCHEMA
			return addPropertyInternal(iPropertyName, iType, iLinkedType, iLinkedClass);
	}

	@Override
	public void fromStream() {
		name = document.field("name");
		shortName = document.field("shortName");
		defaultClusterId = (Integer) document.field("defaultClusterId");

		if (document.field("overSize") != null)
			overSize = (Float) document.field("overSize");

		final Object cc = document.field("clusterIds");
		if (cc instanceof Collection<?>) {
			final Collection<Integer> coll = document.field("clusterIds");
			clusterIds = new int[coll.size()];
			int i = 0;
			for (final Integer item : coll)
				clusterIds[i++] = item.intValue();
		} else
			clusterIds = (int[]) cc;
		Arrays.sort(clusterIds);

		setPolymorphicClusterIds(clusterIds);

		// READ PROPERTIES
		OPropertyImpl prop;
		Collection<ODocument> storedProperties = document.field("properties");
		for (ODocument p : storedProperties) {
			prop = new OPropertyImpl(this, p);
			prop.fromStream();
			properties.put(prop.getName().toLowerCase(), prop);
		}
	}

	@Override
	@OBeforeSerialization
	public ODocument toStream() {
		document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		try {
			document.field("name", name);
			document.field("shortName", shortName);
			document.field("defaultClusterId", defaultClusterId);
			document.field("clusterIds", clusterIds);
			document.field("overSize", overSize);

			final Set<ODocument> props = new HashSet<ODocument>();
			for (final OProperty p : properties.values()) {
				props.add(((OPropertyImpl) p).toStream());
			}
			document.field("properties", props, OType.EMBEDDEDSET);

			if (superClass != null)
				document.field("superClass", superClass.getName());

		} finally {
			document.setInternalStatus(ORecordElement.STATUS.LOADED);
		}

		return document;
	}

	public Class<?> getJavaClass() {
		return javaClass;
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public void setDefaultClusterId(final int iDefaultClusterId) {
		this.defaultClusterId = iDefaultClusterId;
		setDirty();
	}

	public int[] getClusterIds() {
		return clusterIds;
	}

	public int[] getPolymorphicClusterIds() {
		return polymorphicClusterIds;
	}

	public OClass addClusterId(final int iId) {
		for (int currId : clusterIds)
			if (currId == iId)
				return this;

		clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
		clusterIds[clusterIds.length - 1] = iId;
		Arrays.sort(clusterIds);
		setDirty();
		return this;
	}

	public OClass removeClusterId(final int iId) {
		boolean found = false;
		for (int clusterId : clusterIds) {
			if (clusterId == iId) {
				found = true;
				break;
			}
		}

		if (found) {
			final int[] newClusterIds = new int[clusterIds.length - 1];
			for (int i = 0, k = 0; i < clusterIds.length; ++i) {
				if (clusterIds[i] == iId)
					// JUMP IT
					continue;

				newClusterIds[k] = clusterIds[i];
				k++;
			}
		}
		return this;
	}

	public OClass setDirty() {
		document.setDirty();
		if (owner != null)
			owner.setDirty();
		return this;
	}

	public Iterator<OClass> getBaseClasses() {
		if (baseClasses == null || baseClasses.size() == 0)
			return null;

		return baseClasses.iterator();
	}

	/**
	 * Adds a base class to the current one. It adds also the base class cluster ids to the polymorphic cluster ids array.
	 * 
	 * @param iBaseClass
	 *          The base class to add.
	 */
	private OClass addBaseClasses(final OClass iBaseClass) {
		if (baseClasses == null)
			baseClasses = new ArrayList<OClass>();

		if (baseClasses.contains(iBaseClass))
			return this;

		baseClasses.add(iBaseClass);

		// ADD CLUSTER IDS OF BASE CLASS TO THIS CLASS AND ALL SUPER-CLASSES
		OClassImpl currentClass = this;
		while (currentClass != null) {
			currentClass.addPolymorphicClusterIds((OClassImpl) iBaseClass);
			currentClass = (OClassImpl) currentClass.getSuperClass();
		}

		return this;
	}

	public float getOverSize() {
		if (overSize > 0)
			// CUSTOM OVERSIZE SETTED
			return overSize;

		if (superClass != null)
			// RETURN THE OVERSIZE OF THE SUPER CLASS
			return superClass.getOverSize();

		// NO OVERSIZE
		return 0;
	}

	public OClass setOverSize(final float overSize) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		final String cmd = String.format("alter class %s oversize %f", name, overSize);
		getDatabase().command(new OCommandSQL(cmd)).execute();
		setOverSizeInternal(overSize);
		return this;
	}

	public void setOverSizeInternal(final float overSize) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
		this.overSize = overSize;
	}

	@Override
	public String toString() {
		return name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((owner == null) ? 0 : owner.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OClassImpl other = (OClassImpl) obj;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		return true;
	}

	public int compareTo(final OClass o) {
		return name.compareTo(o.getName());
	}

	public long count() {
		return count(true);
	}

	public long count(final boolean iPolymorphic) {
		if (iPolymorphic)
			return getDatabase().countClusterElements(polymorphicClusterIds);

		return getDatabase().countClusterElements(clusterIds);
	}

	private ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	/**
	 * Truncates all the clusters the class uses.
	 * 
	 * @throws IOException
	 */
	public void truncate() throws IOException {
		for (int id : clusterIds) {
			getDatabase().getStorage().getClusterById(id).truncate();
		}
	}

	/**
	 * Returns true if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClassName
	 * @return
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final String iClassName) {
		return isSubClassOf(owner.getClass(iClassName));
	}

	/**
	 * Returns true if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClass
	 * @return
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final OClass iClass) {
		if (iClass == null)
			return false;

		OClass cls = this;
		while (cls != null) {
			if (cls.getName().equals(iClass.getName()))
				return true;
			cls = cls.getSuperClass();
		}
		return false;
	}

	/**
	 * Returns true if the passed schema class (iClass) extends the current instance.
	 * 
	 * @param iClass
	 * @return Returns true if the passed schema class extends the current instance
	 * @see #isSubClassOf(OClass)
	 */
	public boolean isSuperClassOf(final OClass iClass) {
		OClass cls = iClass;
		while (cls != null) {
			if (cls.getName().equals(name))
				return true;
			cls = cls.getSuperClass();
		}
		return false;
	}

	public Object get(final ATTRIBUTES iAttribute) {
		if (iAttribute == null)
			throw new IllegalArgumentException("attribute is null");

		switch (iAttribute) {
		case NAME:
			return getName();
		case SHORTNAME:
			return getShortName();
		case SUPERCLASS:
			return getSuperClass();
		case OVERSIZE:
			return getOverSize();
		}

		throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
	}

	public void setInternalAndSave(final ATTRIBUTES attribute, final Object iValue) {
		if (attribute == null)
			throw new IllegalArgumentException("attribute is null");

		final String stringValue = iValue != null ? iValue.toString() : null;

		switch (attribute) {
		case NAME:
			setNameInternal(stringValue);
			break;
		case SHORTNAME:
			setShortNameInternal(stringValue);
			break;
		case SUPERCLASS:
			setSuperClassInternal(getDatabase().getMetadata().getSchema().getClass(stringValue));
			break;
		case OVERSIZE:
			setOverSizeInternal(Float.parseFloat(stringValue.replace(',', '.')));
			break;
		}

		saveInternal();
	}

	public OClass set(final ATTRIBUTES attribute, final Object iValue) {
		if (attribute == null)
			throw new IllegalArgumentException("attribute is null");

		final String stringValue = iValue != null ? iValue.toString() : null;

		switch (attribute) {
		case NAME:
			setName(stringValue);
			break;
		case SHORTNAME:
			setShortName(stringValue);
			break;
		case SUPERCLASS:
			setSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
			break;
		case OVERSIZE:
			setOverSize(Float.parseFloat(stringValue));
			break;
		}
		return this;
	}

	/**
	 * Add different cluster ids to the "polymorphic cluster ids" array.
	 */
	private void addPolymorphicClusterIds(final OClassImpl iBaseClass) {
		boolean found;
		for (int i : iBaseClass.polymorphicClusterIds) {
			found = false;
			for (int k : clusterIds) {
				if (i == k) {
					found = true;
					break;
				}
			}

			if (!found) {
				// ADD IT
				polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
				polymorphicClusterIds[polymorphicClusterIds.length - 1] = i;
				Arrays.sort(polymorphicClusterIds);
			}
		}
	}

	public OPropertyImpl addPropertyInternal(final String iName, final OType iType, final OType iLinkedType, final OClass iLinkedClass) {
		if (iName == null || iName.length() == 0)
			throw new OSchemaException("Found property name null");

		final Character wrongCharacter = OSchemaShared.checkNameIfValid(iName);
		if (wrongCharacter != null)
			throw new OSchemaException("Invalid property name found. Character '" + wrongCharacter + "' cannot be used in property name.");

		final String lowerName = iName.toLowerCase();

		if (properties.containsKey(lowerName))
			throw new OSchemaException("Class " + name + " already has property '" + iName + "'");

		final OPropertyImpl prop = new OPropertyImpl(this, iName, iType);

		properties.put(lowerName, prop);
		fixedSize += iType.size;

		if (iLinkedType != null)
			prop.setLinkedTypeInternal(iLinkedType);
		else if (iLinkedClass != null)
			prop.setLinkedClassInternal(iLinkedClass);
		return prop;
	}

	public void saveInternal() {
		owner.saveInternal();
	}

	public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
		return createIndex(iName, iType, null, fields);
	}

	public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
			final String... fields) {
		if (!(INDEX_TYPE.DICTIONARY.equals(iType) || INDEX_TYPE.FULLTEXT.equals(iType) || INDEX_TYPE.NOTUNIQUE.equals(iType) || INDEX_TYPE.UNIQUE
				.equals(iType)))
			throw new OIndexException("Index of this type (" + iType + ") cannot be used in class indexes.");

		if (fields.length == 0)
			throw new OIndexException("List of fields to index cannot be empty.");

		if (fields.length > 1 && INDEX_TYPE.FULLTEXT.equals(iType))
			throw new OIndexException(INDEX_TYPE.FULLTEXT + " indexes cannot be used as composite ones.");

		final Set<String> existingFieldNames = properties.keySet();
		final List<String> fieldsToIndex = new LinkedList<String>();

		for (final String fieldToIndex : fields) {
			final String fieldName = extractFieldName( fieldToIndex );
			if (!existingFieldNames.contains(fieldName.toLowerCase()))
				throw new OIndexException("Index with name : '" + iName + "' cannot be created on class : '" + name + "' because field: '"
						+ fieldName + "' is absent in class definition.");

			fieldsToIndex.add(fieldName);
		}

		final OIndexDefinition indexDefinition;
		if (fieldsToIndex.size() == 1) {
      indexDefinition = createSingleFieldIndexDefinition(fields[0] );
    }
		else {
      indexDefinition = createMultipleFieldIndexDefinition( fieldsToIndex );
		}

		final OIndex<?> index = getDatabase().getMetadata().getIndexManager()
				.createIndex(iName, iType.toString(), indexDefinition, clusterIds, iProgressListener);
		return index;
	}

  private OIndexDefinition createMultipleFieldIndexDefinition( final List<String> fieldsToIndex )
  {
    final OIndexDefinition indexDefinition;
    final OCompositeIndexDefinition compositeIndex = new OCompositeIndexDefinition(name);

    for (final String fieldName : fieldsToIndex) {
      final OProperty propertyToIndex = properties.get(fieldName.toLowerCase());
      final OType propertyType = propertyToIndex.getType();
      if (propertyType.equals(OType.EMBEDDEDLIST) || propertyType.equals(OType.EMBEDDEDSET) || propertyType.equals(OType.LINKSET)
          || propertyType.equals(OType.LINKSET) || propertyType.equals( OType.EMBEDDEDMAP ) || propertyType.equals( OType.LINKMAP ))
        throw new OIndexException("Collections are not supported in composite indexes");

      final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(name, propertyToIndex.getName(), propertyType);
      compositeIndex.addIndex(propertyIndex);
    }

    indexDefinition = compositeIndex;
    return indexDefinition;
  }

  private OIndexDefinition createSingleFieldIndexDefinition( final String field )
  {
    final String fieldName = extractFieldName( field );
    final OIndexDefinition indexDefinition;
 
    final OProperty propertyToIndex = properties.get(fieldName.toLowerCase());
    final OType propertyToIndexType = propertyToIndex.getType();
    final OType indexType;
    
    if( propertyToIndexType == OType.EMBEDDEDMAP || propertyToIndexType == OType.LINKMAP) {
      final OPropertyMapIndexDefinition.INDEX_BY indexBy = extractMapIndexSpecifier( field );

      if(indexBy.equals( OPropertyMapIndexDefinition.INDEX_BY.KEY ))
        indexType = OType.STRING;
      else {
        if ( propertyToIndexType == OType.LINKMAP)
          indexType = OType.LINK;
        else
          indexType = propertyToIndex.getLinkedType();
      }

      indexDefinition = new OPropertyMapIndexDefinition(name, propertyToIndex.getName(), indexType, indexBy);
    } else if ( propertyToIndexType.equals( OType.EMBEDDEDLIST ) || propertyToIndexType.equals( OType.EMBEDDEDSET ) ||
      propertyToIndexType.equals( OType.LINKLIST ) || propertyToIndexType.equals( OType.LINKSET )) {
      if(propertyToIndexType.equals( OType.LINKLIST ) || propertyToIndexType.equals( OType.LINKSET ))
        indexType = OType.LINK;
      else 
        indexType = propertyToIndex.getLinkedType();
      
      indexDefinition = new OPropertyListIndexDefinition(name, propertyToIndex.getName(), indexType);
    } else 
      indexDefinition = new OPropertyIndexDefinition( name, propertyToIndex.getName(), propertyToIndexType );
    return indexDefinition;
  }

  public boolean areIndexed(final String... fields) {
		return areIndexed(Arrays.asList(fields));
	}

	public boolean areIndexed(final Collection<String> fields) {
		final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

		final boolean currentClassResult = indexManager.areIndexed(name, fields);

		if (superClass != null)
			return currentClassResult || superClass.areIndexed(fields);
		return currentClassResult;
	}

	public Set<OIndex<?>> getInvolvedIndexes(final String... fields) {
		return getInvolvedIndexes(Arrays.asList(fields));
	}

	public Set<OIndex<?>> getInvolvedIndexes(final Collection<String> fields) {
		final Set<OIndex<?>> result = new HashSet<OIndex<?>>(getClassInvolvedIndexes(fields));

		if (superClass != null)
			result.addAll(superClass.getInvolvedIndexes(fields));

		return result;
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final Collection<String> fields) {
		final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

		return indexManager.getClassInvolvedIndexes(name, fields);
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final String... fields) {
		return getClassInvolvedIndexes(Arrays.asList(fields));
	}

	public OIndex<?> getClassIndex(final String iName) {
		final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

		return indexManager.getClassIndex(name, iName);
	}

	public Set<OIndex<?>> getClassIndexes() {
		final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

		return indexManager.getClassIndexes(name);
	}

	public Set<OIndex<?>> getIndexes() {
		final Set<OIndex<?>> indexes = getClassIndexes();
		if (superClass == null)
			return indexes;

		final Set<OIndex<?>> result = new HashSet<OIndex<?>>(indexes);
		result.addAll(superClass.getIndexes());

		return result;
	}

  private String extractFieldName(final String fieldName) {
    String[] fieldNameParts = fieldName.split( "\\s+" );
    if(fieldNameParts.length == 1)
      return fieldName;
    if(fieldNameParts.length == 3)
      return fieldNameParts[0];

    throw new IllegalArgumentException( "Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName + "'" );
  }

  private OPropertyMapIndexDefinition.INDEX_BY extractMapIndexSpecifier(final String fieldName) {
    String[] fieldNameParts = fieldName.split( "\\s+" );
    if(fieldNameParts.length == 1)
      return OPropertyMapIndexDefinition.INDEX_BY.KEY;

    if(fieldNameParts.length == 3) {
      if("by".equals(fieldNameParts[1].toLowerCase()))
        try{
          return OPropertyMapIndexDefinition.INDEX_BY.valueOf( fieldNameParts[2].toUpperCase() );
        } catch( IllegalArgumentException iae ) {
          throw new IllegalArgumentException( "Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName + "'");
        }
    }

    throw new IllegalArgumentException( "Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName + "'" );
  }

	private void setPolymorphicClusterIds(final int[] iClusterIds) {
		polymorphicClusterIds = iClusterIds;
		Arrays.sort(polymorphicClusterIds);
	}

	private void setClusterIds(final int[] iClusterIds) {
		clusterIds = iClusterIds;
		Arrays.sort(clusterIds);
	}
}
