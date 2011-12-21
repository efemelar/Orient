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
package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Index implementation bound to one schema class property.
 * 
 */
public class OPropertyIndexDefinition extends ODocumentWrapperNoClass implements OIndexDefinition {
	protected String	className;
	protected String	field;
	protected OType		keyType;

	public OPropertyIndexDefinition(final String iClassName, final String iField, final OType iType) {
		super(new ODocument());

		className = iClassName;
		field = iField;
		keyType = iType;
	}

	/**
	 * Constructor used for index unmarshalling.
	 */
	public OPropertyIndexDefinition() {
	}

	public String getClassName() {
		return className;
	}

	public List<String> getFields() {
		return Collections.singletonList(field);
	}

	public Object getDocumentValueToIndex(final ODocument iDocument) {
		return createValue(iDocument.field(field));
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		final OPropertyIndexDefinition that = (OPropertyIndexDefinition) o;

		if (!className.equals(that.className))
			return false;
		if (!field.equals(that.field))
			return false;
		if (keyType != that.keyType)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = className.hashCode();
		result = 31 * result + field.hashCode();
		result = 31 * result + keyType.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "OPropertyIndexDefinition{" + "className='" + className + '\'' + ", field='" + field + '\'' + ", keyType=" + keyType
				+ '}';
	}

	public Object createValue(final List<?> params) {
		return OType.convert(params.get(0), keyType.getDefaultJavaType());
	}

	/**
	 * {@inheritDoc}
	 */
	public Object createValue(final Object... params) {
		return OType.convert( params[0], keyType.getDefaultJavaType() );
	}

	public int getParamCount() {
		return 1;
	}

	public OType[] getTypes() {
		return new OType[] { keyType };
	}

	@Override
	protected final void fromStream() {
    serializeFromStream();
	}

  @Override
  public final ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      serializeToStream();
    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }

    return document;
  }

  protected void serializeToStream() {
    document.field("className", className);
    document.field("field", field);
    document.field("keyType", keyType.toString());
  }

  protected void serializeFromStream() {
    className = document.field("className");
    field = document.field("field");

    final String keyTypeStr = document.field("keyType");
    keyType = OType.valueOf(keyTypeStr);
  }


  /**
	 * {@inheritDoc}
	 * 
	 * @param indexName
	 * @param indexType
	 */
	public String toCreateIndexDDL(final String indexName, final String indexType) {
    final StringBuilder ddl = new StringBuilder("create index ");

    final String shortName = className + "." + field;
    if (indexName.equalsIgnoreCase(shortName)) {
      ddl.append(shortName).append(" ");
    } else {
      ddl.append(indexName).append(" on ");
      ddl.append(className).append(" ( ").append(field).append(" ) ");
    }
    ddl.append(indexType);

    return ddl.toString();
	}
}
