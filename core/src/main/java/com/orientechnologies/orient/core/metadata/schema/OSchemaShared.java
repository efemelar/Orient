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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Shared schema class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaShared extends ODocumentWrapperNoClass implements OSchema, OCloseable {
	public static final int								CURRENT_VERSION_NUMBER	= 4;
	private static final String						DROP_INDEX_QUERY				= "drop index ";
	protected Map<String, OClass>					classes									= new HashMap<String, OClass>();
	private final OSharedResourceExternal	lock										= new OSharedResourceExternal();

	public OSchemaShared(final int schemaClusterId) {
		super(new ODocument());
	}

	public int countClasses() {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
		lock.acquireSharedLock();
		try {
			return classes.size();
		} finally {
			lock.releaseSharedLock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.Class)
	 */
	public OClass createClass(final Class<?> iClass) {
		final Class<?> superClass = iClass.getSuperclass();
		final OClass cls;
		if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
			cls = getClass(superClass.getSimpleName());
		else
			cls = null;

		return createClass(iClass.getSimpleName(), cls, OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.Class, int)
	 */
	public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
		final Class<?> superClass = iClass.getSuperclass();
		final OClass cls;
		if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
			cls = getClass(superClass.getSimpleName());
		else
			cls = null;

		return createClass(iClass.getSimpleName(), cls, iDefaultClusterId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String)
	 */
	public OClass createClass(final String iClassName) {
		return createClass(iClassName, null, OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String,
	 * com.orientechnologies.orient.core.metadata.schema.OClass)
	 */
	public OClass createClass(final String iClassName, final OClass iSuperClass) {
		return createClass(iClassName, iSuperClass, OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String,
	 * com.orientechnologies.orient.core.metadata.schema.OClass, com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE)
	 */
	public OClass createClass(final String iClassName, final OClass iSuperClass, final OStorage.CLUSTER_TYPE iType) {
		if (getDatabase().getTransaction().isActive())
			throw new IllegalStateException("Cannot create a new class inside a transaction");

		int clusterId = getDatabase().getClusterIdByName(iClassName);
		if (clusterId == -1) {
			// CREATE A NEW CLUSTER
			clusterId = getDatabase().addCluster(iClassName, iType);
		}

		return createClass(iClassName, iSuperClass, clusterId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String, int)
	 */
	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		return createClass(iClassName, null, new int[] { iDefaultClusterId });
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String,
	 * com.orientechnologies.orient.core.metadata.schema.OClass, int)
	 */
	public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId) {
		return createClass(iClassName, iSuperClass, new int[] { iDefaultClusterId });
	}

	public OClass getOrCreateClass(final String iClassName) {
		lock.acquireExclusiveLock();
		try {

			OClass cls = classes.get(iClassName.toLowerCase());
			if (cls == null)
				cls = createClass(iClassName);

			return cls;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#createClass(java.lang.String,
	 * com.orientechnologies.orient.core.metadata.schema.OClass, int[])
	 */
	public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

		final String key = iClassName.toLowerCase();

		lock.acquireExclusiveLock();
		try {
			if (classes.containsKey(key))
				throw new OSchemaException("Class " + iClassName + " already exists in current database");

			final StringBuilder cmd = new StringBuilder("create class ");
			cmd.append(iClassName);

			if (iSuperClass != null) {
				cmd.append(" extends ");
				cmd.append(iSuperClass.getName());
			}

			if (iClusterIds != null) {
				cmd.append(" cluster ");
				for (int i = 0; i < iClusterIds.length; ++i) {
					if (i > 0)
						cmd.append(',');
					else
						cmd.append(' ');

					cmd.append(iClusterIds[i]);
				}
			}

			getDatabase().command(new OCommandSQL(cmd.toString())).execute();
			getDatabase().reload();

			if (classes.containsKey(key))
				return classes.get(key);
			else
				// ADD IT LOCALLY AVOIDING TO RELOAD THE ENTIRE SCHEMA
				createClassInternal(iClassName, iSuperClass, iClusterIds);

			return classes.get(key);

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public OClass createClassInternal(final String iClassName, final OClass iSuperClass, int[] iClusterIds) {
		if (iClassName == null || iClassName.length() == 0)
			throw new OSchemaException("Found class name null");

		final Character wrongCharacter = checkNameIfValid(iClassName);
		if (wrongCharacter != null)
			throw new OSchemaException("Found invalid class name. Character '" + wrongCharacter + "' cannot be used in class name.");

		if (iClusterIds == null || iClusterIds.length == 0)
			// CREATE A NEW CLUSTER
			iClusterIds = new int[] { getDatabase().addCluster(iClassName, CLUSTER_TYPE.PHYSICAL) };

		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

		final String key = iClassName.toLowerCase();

		lock.acquireExclusiveLock();
		try {
			if (classes.containsKey(key))
				throw new OSchemaException("Class " + iClassName + " already exists in current database");

			final OClassImpl cls = new OClassImpl(this, iClassName, iClusterIds);
			classes.put(key, cls);

			if (cls.getShortName() != null)
				// BIND SHORT NAME TOO
				classes.put(cls.getShortName().toLowerCase(), cls);

			if (iSuperClass != null)
				cls.setSuperClassInternal(iSuperClass);

			return cls;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public static Character checkNameIfValid(String iName) {
		if (iName == null)
			throw new IllegalArgumentException("Name is null");

		iName = iName.trim();

		if (iName.length() == 0)
			throw new IllegalArgumentException("Name is empty");

		for (int i = 0; i < iName.length(); ++i) {
			final char c = iName.charAt(i);
			if (c == ':' || c == ',')
				// INVALID CHARACTER
				return c;
		}

		// for (char c : iName.toCharArray())
		// if (!Character.isJavaIdentifierPart(c))
		// return c;
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#dropClass(java.lang.String)
	 */
	public void dropClass(final String iClassName) {
		if (iClassName == null)
			throw new IllegalArgumentException("Class name is null");

		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

		final String key = iClassName.toLowerCase();

		lock.acquireExclusiveLock();
		try {

			final OClass cls = classes.get(key);
			if (cls == null)
				throw new OSchemaException("Class " + iClassName + " was not found in current database");

			if (cls.getBaseClasses() != null)
				throw new OSchemaException("Class " + iClassName
						+ " cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

			final StringBuilder cmd = new StringBuilder("drop class ");
			cmd.append(iClassName);

			getDatabase().command(new OCommandSQL(cmd.toString())).execute();
			getDatabase().reload();
			reload();

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void dropClassInternal(final String iClassName) {
		if (iClassName == null)
			throw new IllegalArgumentException("Class name is null");

		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

		final String key = iClassName.toLowerCase();

		lock.acquireExclusiveLock();
		try {

			final OClass cls = classes.get(key);
			if (cls == null)
				throw new OSchemaException("Class " + iClassName + " was not found in current database");

			if (cls.getBaseClasses() != null)
				throw new OSchemaException("Class " + iClassName
						+ " cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

			if (cls.getSuperClass() != null) {
				// REMOVE DEPENDENCY FROM SUPERCLASS
				((OClassImpl) cls.getSuperClass()).baseClasses.remove(cls);
			}

			dropClassIndexes(cls);

			classes.remove(key);
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	private void dropClassIndexes(final OClass cls) {
		for (final OIndex<?> index : getDatabase().getMetadata().getIndexManager().getClassIndexes(cls.getName())) {
			getDatabase().command(new OCommandSQL(DROP_INDEX_QUERY + index.getName()));
		}
	}

	/**
	 * Reloads the schema inside a storage's shared lock.
	 */
	@Override
	public <RET extends ODocumentWrapper> RET reload() {
		getDatabase().getStorage().callInLock(new Callable<Void>() {

			public Void call() throws Exception {
				lock.acquireExclusiveLock();
				try {
					reload(null);
				} finally {
					lock.releaseExclusiveLock();
				}

				return null;
			}
		}, false);

		return (RET) this;
	}

	public boolean existsClass(final String iClassName) {
		lock.acquireSharedLock();
		try {

			return classes.containsKey(iClassName.toLowerCase());

		} finally {
			lock.releaseSharedLock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.Class)
	 */
	public OClass getClass(final Class<?> iClass) {
		return getClass(iClass.getSimpleName());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.String)
	 */
	public OClass getClass(final String iClassName) {
		if (iClassName == null)
			return null;

		OClass cls;

		lock.acquireSharedLock();
		try {

			cls = classes.get(iClassName.toLowerCase());

		} finally {
			lock.releaseSharedLock();
		}

		if (cls == null) {
			// CHECK IF CAN AUTO-CREATE IT
			final ODatabase ownerDb = getDatabase().getDatabaseOwner();
			if (ownerDb instanceof ODatabaseObjectTx) {
				final Class<?> javaClass = ((ODatabaseObjectTx) ownerDb).getEntityManager().getEntityClass(iClassName);

				if (javaClass != null) {
					// AUTO REGISTER THE CLASS AT FIRST USE
					cls = cascadeCreate(javaClass);
				}
			}
		}
		return cls;
	}

	public void changeClassName(String iOldName, String iNewName) {
		OClass clazz = classes.remove(iOldName.toLowerCase());
		classes.put(iNewName.toLowerCase(), clazz);
	}

	/**
	 * Binds ODocument to POJO.
	 */
	@Override
	public void fromStream() {
		// READ CURRENT SCHEMA VERSION
		int schemaVersion = (Integer) document.field("schemaVersion");
		if (schemaVersion != CURRENT_VERSION_NUMBER) {
			// HANDLE SCHEMA UPGRADE
			throw new OConfigurationException(
					"Database schema is different. Please export your old database with the previous version of OrientDB and reimport it using the current one.");
		}

		// REGISTER ALL THE CLASSES
		classes.clear();
		OClassImpl cls;
		Collection<ODocument> storedClasses = document.field("classes");
		for (ODocument c : storedClasses) {
			cls = new OClassImpl(this, c);
			cls.fromStream();
			classes.put(cls.getName().toLowerCase(), cls);

			if (cls.getShortName() != null)
				classes.put(cls.getShortName().toLowerCase(), cls);
		}

		// REBUILD THE INHERITANCE TREE
		String superClassName;
		OClass superClass;
		for (ODocument c : storedClasses) {
			superClassName = c.field("superClass");

			if (superClassName != null) {
				// HAS A SUPER CLASS
				cls = (OClassImpl) classes.get(((String) c.field("name")).toLowerCase());

				superClass = classes.get(superClassName.toLowerCase());

				if (superClass == null)
					throw new OConfigurationException("Super class '" + superClassName + "' was declared in class '" + cls.getName()
							+ "' but was not found in schema. Remove the dependency or create the class to continue.");

				cls.setSuperClassInternal(superClass);
			}
		}
	}

	/**
	 * Binds POJO to ODocument.
	 */
	@Override
	@OBeforeSerialization
	public ODocument toStream() {
		document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		try {
			document.field("schemaVersion", CURRENT_VERSION_NUMBER);

			Set<ODocument> cc = new HashSet<ODocument>();
			for (OClass c : classes.values()) {
				cc.add(((OClassImpl) c).toStream());
			}
			document.field("classes", cc, OType.EMBEDDEDSET);

		} finally {
			document.setInternalStatus(ORecordElement.STATUS.LOADED);
		}

		return document;
	}

	public Collection<OClass> getClasses() {
		getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
		lock.acquireSharedLock();
		try {
			return new HashSet<OClass>(classes.values());
		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public OSchemaShared load() {
		lock.acquireExclusiveLock();
		try {

			getDatabase();
			((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().schemaRecordId);
			super.reload("*:-1 index:0");
			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void create() {
		final ODatabaseRecord db = getDatabase();
		super.save(OStorage.CLUSTER_INTERNAL_NAME);
		db.getStorage().getConfiguration().schemaRecordId = document.getIdentity().toString();
		db.getStorage().getConfiguration().update();
	}

	public void saveInternal() {
		getDatabase();

		if (document.getDatabase().getTransaction().isActive())
			throw new OSchemaException("Cannot change the schema while a transaction is active. Schema changes are not transactional");

		lock.acquireExclusiveLock();
		try {

			document.setDirty();
			super.save();

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public int getVersion() {
		lock.acquireSharedLock();
		try {

			return document.getVersion();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public ORID getIdentity() {
		return document.getIdentity();
	}

	/**
	 * Avoid to handle this by user API.
	 */
	@Override
	public <RET extends ODocumentWrapper> RET save() {
		return (RET) this;
	}

	/**
	 * Avoid to handle this by user API.
	 */
	@Override
	public <RET extends ODocumentWrapper> RET save(final String iClusterName) {
		return (RET) this;
	}

	public OSchemaShared setDirty() {
		document.setDirty();
		return this;
	}

	private OClass cascadeCreate(final Class<?> javaClass) {
		final OClassImpl cls = (OClassImpl) createClass(javaClass.getSimpleName());

		final Class<?> javaSuperClass = javaClass.getSuperclass();
		if (javaSuperClass != null && !javaSuperClass.getName().equals("java.lang.Object")) {
			OClass superClass = classes.get(javaSuperClass.getSimpleName().toLowerCase());
			if (superClass == null)
				superClass = cascadeCreate(javaSuperClass);
			cls.setSuperClass(superClass);
		}

		return cls;
	}

	private ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public void close() {
		classes.clear();
		document.reset();
	}
}
