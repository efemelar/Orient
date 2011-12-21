package com.orientechnologies.orient.test.database.auto;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

@Test(groups = { "index" })
public class SQLCreateIndexTest {

	private final ODatabaseDocumentTx	database;
	private static final OType				EXPECTED_PROP1_TYPE	= OType.DOUBLE;
	private static final OType				EXPECTED_PROP2_TYPE	= OType.INTEGER;

	@Parameters(value = "url")
	public SQLCreateIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeClass
	public void beforeClass() {
		if (database.isClosed())
			database.open("admin", "admin");

		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.createClass("sqlCreateIndexTestClass");
		oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
		oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);
    oClass.createProperty( "prop3", OType.EMBEDDEDMAP, OType.INTEGER );
    oClass.createProperty( "prop4", OType.LINKMAP);

		schema.save();
		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		if (database.isClosed())
			database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() {
		database.close();
	}

	@AfterClass
	public void afterClass() throws Exception {
		if (database.isClosed())
			database.open("admin", "admin");

		database.command(new OCommandSQL("delete from sqlCreateIndexTestClass")).execute();
		database.command(new OCommandSQL("drop class sqlCreateIndexTestClass")).execute();
    database.getMetadata().getSchema().reload();
    database.getLevel2Cache().clear();
		database.close();
	}

	@Test
	public void testOldSyntax() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexTestClass.prop1 UNIQUE")).execute();

		database.getMetadata().getIndexManager().reload();
		final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
				.getClassIndex("sqlCreateIndexTestClass.prop1");

		Assert.assertNotNull(index);

		final OIndexDefinition indexDefinition = index.getDefinition();

		Assert.assertTrue(indexDefinition instanceof OPropertyIndexDefinition);
		Assert.assertEquals(indexDefinition.getFields().get(0), "prop1");
		Assert.assertEquals(indexDefinition.getTypes()[0], EXPECTED_PROP1_TYPE);
		Assert.assertEquals(index.getType(), "UNIQUE");
	}

	@Test
	public void testCreateIndexWithoutClass() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexWithoutClass UNIQUE double")).execute();
		database.getMetadata().getIndexManager().reload();

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("sqlCreateIndexWithoutClass");

		Assert.assertNotNull(index);
		Assert.assertEquals(index.getKeyTypes(), new OType[] { OType.DOUBLE });
		Assert.assertEquals(index.getType(), "UNIQUE");
	}

	@Test
	public void testCreateCompositeIndex() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexCompositeIndex ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
				.getClassIndex("sqlCreateIndexCompositeIndex");

		Assert.assertNotNull(index);

		final OIndexDefinition indexDefinition = index.getDefinition();

		Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
		Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
		Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
		Assert.assertEquals(index.getType(), "UNIQUE");
	}

  @Test
  public void testCreateEmbeddedMapIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexEmbeddedMapIndex ON sqlCreateIndexTestClass (prop3) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexEmbeddedMapIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.STRING });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexOne() throws Exception
  {
    try {
      database.command( new OCommandSQL( "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by ttt) UNIQUE" ) )
        .execute();
      Assert.fail();
    } catch( OCommandSQLParsingException e ) {
      Assert
        .assertTrue( e
          .getMessage()
          .contains(
            "Error on parsing command at position #84: Illegal field name format, should be '<property> [by key|value]' but was 'PROP3 BY TTT'\n"
              + "Command: CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by ttt) UNIQUE\n"
              + "--------------------------------------------------------------------------------------------^" ) );
    }
    final OIndex<?> index = database.getMetadata().getSchema().getClass( "sqlCreateIndexTestClass" )
      .getClassIndex( "sqlCreateIndexEmbeddedMapWrongSpecifierIndex" );

    Assert.assertNull( index, "Index created while wrong query was executed" );
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexTwo() throws Exception
  {
    try {
      database.command( new OCommandSQL( "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 b value) UNIQUE" ) )
        .execute();
      Assert.fail();
    } catch( OCommandSQLParsingException e ) {
      Assert
        .assertTrue( e
          .getMessage()
          .contains(
            "Error on parsing command at position #84: Illegal field name format, should be '<property> [by key|value]' but was 'PROP3 B VALUE'\n"
              + "Command: CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 b value) UNIQUE\n"
              + "--------------------------------------------------------------------------------------------^" ) );
    }
    final OIndex<?> index = database.getMetadata().getSchema().getClass( "sqlCreateIndexTestClass" )
      .getClassIndex( "sqlCreateIndexEmbeddedMapWrongSpecifierIndex" );

    Assert.assertNull( index, "Index created while wrong query was executed" );
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexThree() throws Exception
  {
    try {
      database.command( new OCommandSQL( "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by value t) UNIQUE" ) )
        .execute();
      Assert.fail();
    } catch( OCommandSQLParsingException e ) {
      Assert
        .assertTrue( e
          .getMessage()
          .contains(
            "Error on parsing command at position #84: Illegal field name format, should be '<property> [by key|value]' but was 'PROP3 BY VALUE T'\n"
              + "Command: CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by value t) UNIQUE\n"
              + "--------------------------------------------------------------------------------------------^" ) );
    }
    final OIndex<?> index = database.getMetadata().getSchema().getClass( "sqlCreateIndexTestClass" )
      .getClassIndex( "sqlCreateIndexEmbeddedMapWrongSpecifierIndex" );

    Assert.assertNull( index, "Index created while wrong query was executed" );
  }


  @Test
  public void testCreateEmbeddedMapByKeyIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexEmbeddedMapByKeyIndex ON sqlCreateIndexTestClass (prop3 by key) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexEmbeddedMapByKeyIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.STRING });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateEmbeddedMapByValueIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexEmbeddedMapByValueIndex ON sqlCreateIndexTestClass (prop3 by value) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexEmbeddedMapByValueIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE );
  }

  @Test
  public void testCreateLinkMapIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexLinkMapIndex ON sqlCreateIndexTestClass (prop4) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexLinkMapIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop4"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.STRING });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateLinkMapByKeyIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexLinkMapByKeyIndex ON sqlCreateIndexTestClass (prop4 by key) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexLinkMapByKeyIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop4"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.STRING });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateLinkMapByValueIndex() throws Exception {
    database.command(new OCommandSQL("CREATE INDEX sqlCreateIndexLinkMapByValueIndex ON sqlCreateIndexTestClass (prop4 by value) UNIQUE"))
      .execute();
    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
      .getClassIndex("sqlCreateIndexLinkMapByValueIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop4"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { OType.LINK });
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals( ((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE );
  }


	@Test
	public void testCreateCompositeIndexWithTypes() throws Exception {
		final String query = new StringBuilder(
				"CREATE INDEX sqlCreateIndexCompositeIndex2 ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE ").append(EXPECTED_PROP1_TYPE)
				.append(", ").append(EXPECTED_PROP2_TYPE).toString();

		database.command(new OCommandSQL(query)).execute();
		database.getMetadata().getIndexManager().reload();

		final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
				.getClassIndex("sqlCreateIndexCompositeIndex2");

		Assert.assertNotNull(index);

		final OIndexDefinition indexDefinition = index.getDefinition();

		Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
		Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
		Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
		Assert.assertEquals(index.getType(), "UNIQUE");
	}

	@Test
	public void testCreateCompositeIndexWithWrongTypes() throws Exception {
		final String query = new StringBuilder(
				"CREATE INDEX sqlCreateIndexCompositeIndex3 ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE ").append(EXPECTED_PROP1_TYPE)
				.append(", ").append(EXPECTED_PROP1_TYPE).toString();

		try {
			database.command(new OCommandSQL(query)).execute();
			Assert.fail();
		} catch (OCommandSQLParsingException e) {
			Assert
					.assertTrue(e
							.getMessage()
							.contains(
									"Error on parsing command at position #91: Property type list not match with real property types\n"
											+ "Command: CREATE INDEX sqlCreateIndexCompositeIndex3 ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE DOUBLE, DOUBLE\n"
											+ "---------------------------------------------------------------------------------------------------^"));
		}
		final OIndex<?> index = database.getMetadata().getSchema().getClass("sqlCreateIndexTestClass")
				.getClassIndex("sqlCreateIndexCompositeIndex3");

		Assert.assertNull(index, "Index created while wrong query was executed");
	}
}
