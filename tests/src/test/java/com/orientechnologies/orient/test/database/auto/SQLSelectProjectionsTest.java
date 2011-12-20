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
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLSelectProjectionsTest {
	private String						url;
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLSelectProjectionsTest(String iURL) {
		url = iURL;
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryProjectionOk() {
		database.open("admin", "admin");

		List<ODocument> result = database
				.command(
						new OSQLSynchQuery<ODocument>(
								" select nick, followings, followers from Profile where nick is defined and followings is defined and followers is defined"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			String[] colNames = d.fieldNames();
			Assert.assertEquals(colNames.length, 3);
			Assert.assertEquals(colNames[0], "nick");
			Assert.assertEquals(colNames[1], "followings");
			Assert.assertEquals(colNames[2], "followers");

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionObjectLevel() {
		ODatabaseObjectTx db = new ODatabaseObjectTx(url);
		db.open("admin", "admin");

		List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>(" select nick, followings, followers from Profile "));

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 3);
			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		db.close();
	}

	@Test
	public void queryProjectionLinkedAndFunction() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select name.toUppercase(), address.city.country.name from Profile")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 2);
			if (d.field("name") != null)
				Assert.assertTrue(d.field("name").equals(((String) d.field("name")).toUpperCase()));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionSameFieldTwice() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select name, name.toUppercase() from Profile where name is not null")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 2);
			Assert.assertNotNull(d.field("name"));
			Assert.assertNotNull(d.field("name2"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionStaticValues() {
		database.open("admin", "admin");

		List<ODocument> result = database
				.command(
						new OSQLSynchQuery<ODocument>(
								"select location.city.country.name, address.city.country.name from Profile where location.city.country.name is not null"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {

			Assert.assertNotNull(d.field("location"));
			Assert.assertNull(d.field("address"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionPrefixAndAppend() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"select *, name.prefix('Mr. ').append(' ').append(surname).append('!') as test from Profile where name is not null"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.field("test").toString(), "Mr. " + d.field("name") + " " + d.field("surname") + "!");

			Assert.assertFalse(d.getIdentity().isValid());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionFunctionsAndFieldOperators() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select max(name.append('.')).prefix('Mr. ') as name from Profile where name is not null"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 1);
			Assert.assertTrue(d.field("name").toString().startsWith("Mr. "));
			Assert.assertTrue(d.field("name").toString().endsWith("."));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionAliases() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"select name.append('!') as 1, surname as 2 from Profile where name is not null and surname is not null")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 2);
			Assert.assertTrue(d.field("1").toString().endsWith("!"));
			Assert.assertNotNull(d.field("2"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionSimpleValues() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select 10, 'ciao' from Profile LIMIT 1")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 2);
			Assert.assertEquals(((Integer) d.field("10")).intValue(), 10l);
			Assert.assertEquals(d.field("ciao"), "ciao");

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionJSON() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select @this.toJson() as json from Profile"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.fieldNames().length <= 1);
			Assert.assertNotNull(d.field("json"));

			new ODocument().fromJSON((String) d.field("json"));
		}

		database.close();
	}

	@Test
	public void queryProjectionContentCollection() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("SELECT FLATTEN( out ) FROM OGraphVertex WHERE out TRAVERSE(1,1) (@class = 'OGraphEdge')"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getClassName(), "OGraphEdge");
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test(expectedExceptions = OCommandSQLParsingException.class)
	public void queryProjectionFlattenError() {
		database.open("admin", "admin");

		try {
			database.command(
					new OSQLSynchQuery<ODocument>(
							"SELECT FLATTEN( out ), in FROM OGraphVertex WHERE out TRAVERSE(1,1) (@class = 'OGraphEdge')")).execute();

		} finally {
			database.close();
		}
	}

	public void queryProjectionRid() {
		database.open("admin", "admin");

		try {
			List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select @rid FROM V")).execute();
			Assert.assertTrue(result.size() != 0);

			for (ODocument d : result) {
				Assert.assertTrue(d.fieldNames().length <= 1);
				Assert.assertNotNull(d.field("rid"));

				final ORID rid = d.field("rid", ORID.class);
				Assert.assertTrue(rid.isValid());
			}

		} finally {
			database.close();
		}
	}

	public void queryProjectionOrigin() {
		database.open("admin", "admin");

		try {
			List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select @raw FROM V")).execute();
			Assert.assertTrue(result.size() != 0);

			for (ODocument d : result) {
				Assert.assertTrue(d.fieldNames().length <= 1);
				Assert.assertNotNull(d.field("raw"));
			}

		} finally {
			database.close();
		}
	}
}
