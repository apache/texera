package edu.uci.ics.textdb.textql.scope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.relation.RelationManager;
import edu.uci.ics.textdb.textql.statements.CreateViewStatement;
import edu.uci.ics.textdb.textql.statements.SelectExtractStatement;
import edu.uci.ics.textdb.textql.statements.Statement;
import edu.uci.ics.textdb.textql.statements.StatementTestUtils;
import edu.uci.ics.textdb.textql.statements.predicates.SelectAllFieldsPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import junit.framework.Assert;

public class ScopeTest {

    /**
     * RelationManager used in the tests 
     */
    private static RelationManager relationManager;
    
    /**
     * Sample tables used in the tests
     */
    private static final String PEOPLE_TABLE = "people";
    private static final Schema PEOPLE_SCHEMA = TestConstants.SCHEMA_PEOPLE;

    private static final String ALL_FIELD_TYPES_TABLE = "allFieldTypes";
    private static final Schema ALL_FIELD_TYPES_SCHEMA = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
    
    private static final String SAMPLE_TABLE0 = "schema00";
    private static final Schema SAMPLE_SCHEMA0 = new Schema(
            new Attribute("X", FieldType.INTEGER),
            new Attribute("Y", FieldType.INTEGER),
            new Attribute("Z", FieldType.STRING)
        );
    
    private static final String SAMPLE_TABLE1 = "schema01";
    private static final Schema SAMPLE_SCHEMA1 = new Schema(
            new Attribute("attr1", FieldType.DATE),
            new Attribute("attr2", FieldType.DOUBLE),
            new Attribute("attr3", FieldType.INTEGER),
            new Attribute("attr4", FieldType.LIST),
            new Attribute("attr5", FieldType.STRING),
            new Attribute("attr6", FieldType.TEXT)
        );
    
    private static final Map<String,Schema> TEST_TABLES = new HashMap<>();
    static {
        TEST_TABLES.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        TEST_TABLES.put(ALL_FIELD_TYPES_TABLE, ALL_FIELD_TYPES_SCHEMA);
        TEST_TABLES.put(SAMPLE_TABLE0, SAMPLE_SCHEMA0);
        TEST_TABLES.put(SAMPLE_TABLE1, SAMPLE_SCHEMA1);
    }
    
    /**
     * Delete all the test tables from the Relation Manager.
     * @throws StorageException If an error occur in the Relation Manager while deleting a table.
     */
    private static void dropTestTables() throws StorageException {
        for(String tableName : TEST_TABLES.keySet()){
            relationManager.deleteTable(tableName);
        }
    }
    
    /**
     * Delete all the test tables from the Relation Manager and insert new test tables.
     * @param tableNames The name of the test tables to be added to the Relation Manager.
     * @throws StorageException If an error occur in the Relation Manager while deleting or creating a table. 
     */
    private static void initTestTables(String... tableNames) throws StorageException {
        dropTestTables();
        String indexesDirectory = "../index/test_tables/";
        for(String tableName : tableNames) {
            String indexDirectory =  indexesDirectory + tableName;
            Schema tableSchema = TEST_TABLES.get(tableName);
            String luceneAnalyzer = LuceneAnalyzerConstants.standardAnalyzerString();
            relationManager.createTable(tableName, indexDirectory, tableSchema, luceneAnalyzer);
        }
    }
    
    
    /**
     * Initialize the Relation Manager.
     * @throws StorageException If an error occur while initializing the Relation Manager.
     */
    @Before
    public void setUp() throws StorageException {
        relationManager = RelationManager.getRelationManager();
    }
    
    /**
     * Delete all the test tables added to the Relation Manager during the tests.
     * @throws StorageException If an error occur while deleting the test tables.
     */
    @After
    public void cleanUp() throws StorageException {
        dropTestTables();
    }
    

    /**
     * Test the Scope by adding a schema and retrieving it.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithoutRelationManager00() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add a schema into the scope
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
        
        // Assert the value of the relation manager
        Assert.assertEquals(null, scope.getRealationManager());
        // Assert the values of the added Schema
        Assert.assertEquals(PEOPLE_SCHEMA, scope.getSchema(PEOPLE_TABLE));
        Assert.assertEquals(Collections.singletonMap(PEOPLE_TABLE, PEOPLE_SCHEMA), scope.getSchemas());
    }

    /**
     * Test the Scope by adding multiple schemas and retrieving them.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithoutRelationManager01() throws TextDBException, StorageException {
        // Build a map of data to be inserted
        Map<String,Schema> schemaById = new HashMap<>();
        schemaById.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        schemaById.put(ALL_FIELD_TYPES_TABLE, ALL_FIELD_TYPES_SCHEMA);
        schemaById.put(SAMPLE_TABLE0, SAMPLE_SCHEMA0);
        schemaById.put(SAMPLE_TABLE1, SAMPLE_SCHEMA1);
        
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add schemas into the scope
        for(Map.Entry<String,Schema> schemaByIdEntry : schemaById.entrySet()){
            scope.addSchema(schemaByIdEntry.getKey(), schemaByIdEntry.getValue());
        }

        // Assert the value of the relation manager
        Assert.assertEquals(null, scope.getRealationManager());
        // Assert the values of the added Schemas (in an order different than they have been inserted)
        ArrayList<Map.Entry<String,Schema>> shuffledSchemaByIdEntries = new ArrayList<>(schemaById.entrySet());
        Collections.shuffle(shuffledSchemaByIdEntries, new Random(0x59));
        for(Map.Entry<String,Schema> schemaByIdEntry : shuffledSchemaByIdEntries){
            Assert.assertEquals(schemaByIdEntry.getValue(), scope.getSchema(schemaByIdEntry.getKey()));
        }
        Assert.assertEquals(schemaById, scope.getSchemas());
    }
    
    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * non existing schema is requested (null should be returned).
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithoutRelationManager02() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Assert the returned value of a schema that has not been added
        Assert.assertEquals(null, scope.getSchema(ALL_FIELD_TYPES_TABLE));
    }

    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Schema with an invalid name (null) is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddSchemaWithoutRelationManager03() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add a schema with a null name
        scope.addSchema(null, PEOPLE_SCHEMA);
    }

    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Schema with an invalid name (empty) is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddSchemaWithoutRelationManager04() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add a schema with an empty("") name
        scope.addSchema("", PEOPLE_SCHEMA);
    }

    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Schema with an already associated name is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddSchemaWithoutRelationManager05() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add schemas into the scope (PEOPLE_TABLE is added twice)
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
    }
    
    /**
     * Test the Scope with a RelationManager by adding a schema, retrieving 
     * the added schema and retrieving a schema from the relation manager.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithRelationManager00() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(SAMPLE_TABLE0);
        Scope scope = new Scope(relationManager);
        
        // Add a schema into the scope
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);

        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        // Assert added Schemas
        Assert.assertEquals(PEOPLE_SCHEMA, scope.getSchema(PEOPLE_TABLE));
        Map<String,Schema> declaredSchemas = new HashMap<>();
        declaredSchemas.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        Assert.assertEquals(declaredSchemas, scope.getSchemas());
        // Assert added Schemas after a test table is requested
        Assert.assertEquals(Utils.getSchemaWithID(SAMPLE_SCHEMA0), scope.getSchema(SAMPLE_TABLE0));
        declaredSchemas.put(SAMPLE_TABLE0, Utils.getSchemaWithID(SAMPLE_SCHEMA0));
        Assert.assertEquals(declaredSchemas, scope.getSchemas());
    }
    
    /**
     * Test the Scope with a RelationManager by adding multiple schemas,
     * retrieving them and retrieving multiple schemas from the relation manager.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithRelationManager01() throws TextDBException, StorageException {
        // Build a map of data to be inserted
        Map<String,Schema> schemaById = new HashMap<>();
        schemaById.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        schemaById.put(ALL_FIELD_TYPES_TABLE, ALL_FIELD_TYPES_SCHEMA);

        // Scope and Tables Initialization
        initTestTables(SAMPLE_TABLE0, SAMPLE_TABLE1);
        Scope scope = new Scope(relationManager);

        // Add a schema into the scope
        for(Map.Entry<String,Schema> schemaByIdEntry : schemaById.entrySet()){
            scope.addSchema(schemaByIdEntry.getKey(), schemaByIdEntry.getValue());
        }

        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        // Assert the values of the added Schemas (in an order different than they have been inserted)
        ArrayList<Map.Entry<String,Schema>> shuffledSchemaByIdEntries = new ArrayList<>(schemaById.entrySet());
        Collections.shuffle(shuffledSchemaByIdEntries, new Random(0x59));
        for(Map.Entry<String,Schema> schemaByIdEntry : shuffledSchemaByIdEntries){
            Assert.assertEquals(schemaByIdEntry.getValue(), scope.getSchema(schemaByIdEntry.getKey()));
        }
        Assert.assertEquals(new HashSet<>(shuffledSchemaByIdEntries), new HashSet<>(scope.getSchemas().entrySet()));
        // Assert the values of the added Schemas after a test table is requested
        Assert.assertEquals(Utils.getSchemaWithID(SAMPLE_SCHEMA1), scope.getSchema(SAMPLE_TABLE1));
        schemaById.put(SAMPLE_TABLE1, Utils.getSchemaWithID(SAMPLE_SCHEMA1));
        Assert.assertEquals(Utils.getSchemaWithID(SAMPLE_SCHEMA0), scope.getSchema(SAMPLE_TABLE0));
        schemaById.put(SAMPLE_TABLE0, Utils.getSchemaWithID(SAMPLE_SCHEMA0));
        Assert.assertEquals(schemaById, scope.getSchemas());
    }
    
    /**
     * Test the behavior of the Scope with a RelationManager when a Schema
     * with the same name of a table in the RelationManager is added (when 
     * the table information has not been retrieved by the Scope yet). A
     * TextDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddSchemaWithRelationManager02() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);
        
        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        
        // Add a schema into the scope with the same name as a table
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
    }
    
    /**
     * Test the behavior of the Scope with a RelationManager when a Schema
     * with the same name of a table in the RelationManager is added (when 
     * the table information has already been retrieved by the Scope). A 
     * TextDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddSchemaWithRelationManager03() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);
        
        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        
        // Make the Scope fetch a table schema and keep it in the collection of schemas 
        Assert.assertEquals(Utils.getSchemaWithID(PEOPLE_SCHEMA), scope.getSchema(PEOPLE_TABLE));
        
        // Add a schema into the scope with the same name as a table
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
    }
    
    /**
     * Test the behavior of the Scope with a RelationManager when a 
     * non existing schema or table is requested (null should be returned).
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddSchemaWithRelationManager04() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);

        // Assert the returned value of a schema that has not been added
        Assert.assertEquals(null, scope.getSchema(ALL_FIELD_TYPES_TABLE));
    }   
    
    /**
     * Test the Scope by adding a Statement, retrieving it and retrieving 
     * its generated Schema.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddStatementWithoutRelationManager00() throws TextDBException, StorageException {
        /*
         * TODO: All implemented statements (CreateViewStatement and SelectExtractStatement), at the moment, have
         * another statement as dependency, thus no values can be added into the scope with only Statements.
         * This test Must be implemented in the future when a Statement can generate a schema by itself. 
         */
    }
    
    /**
     * Test the Scope by adding a Statement that require an added Schema,
     * retrieving it and retrieve its generated Schema.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddStatementWithoutRelationManager01() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add a schema into the scope
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
        // Build and add a statement into the scope
        String statementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        Schema selectExtractStatementOutputSchema = selectExtractStatement.generateOutputSchema(PEOPLE_SCHEMA);
        scope.addStatement(selectExtractStatement);

        // Assert the value of the relation manager
        Assert.assertEquals(null, scope.getRealationManager());
        // Assert added Schemas
        Assert.assertEquals(PEOPLE_SCHEMA, scope.getSchema(PEOPLE_TABLE));
        Assert.assertEquals(selectExtractStatementOutputSchema, scope.getSchema(statementId));
        Map<String,Schema> declaredSchemas = new HashMap<>();
        declaredSchemas.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        declaredSchemas.put(statementId, selectExtractStatementOutputSchema);
        Assert.assertEquals(declaredSchemas, scope.getSchemas());
        // Assert added Statements
        Set<Statement> declaredStatements = Collections.singleton(selectExtractStatement);
        Assert.assertEquals(declaredStatements, new HashSet<>(scope.getStatements()));
    }
    
    /**
     * Test the Scope by adding a Statement that require an added Statement,
     * retrieving it and retrieve its generated Schema.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddStatementWithoutRelationManager02() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Add schemas into the scope
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
        scope.addSchema(SAMPLE_TABLE0, SAMPLE_SCHEMA0);
        // Build and add statements into the scope
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        String statmentId0 = "_sid0";
        SelectExtractStatement selectExtractStatement0 = new SelectExtractStatement(statmentId0, selectPredicate, null, PEOPLE_TABLE, null, null);
        Schema selectExtractStatement0OutputSchema = selectExtractStatement0.generateOutputSchema(PEOPLE_SCHEMA);
        String statmentId1 = "_sid1";
        SelectExtractStatement selectExtractStatement1 = new SelectExtractStatement(statmentId1, selectPredicate, null, statmentId0, null, null);
        Schema selectExtractStatement1OutputSchema = selectExtractStatement1.generateOutputSchema(selectExtractStatement0OutputSchema);
        scope.addStatement(selectExtractStatement0);
        scope.addStatement(selectExtractStatement1);

        // Assert the value of the relation manager
        Assert.assertEquals(null, scope.getRealationManager());
        // Assert added Schemas
        Assert.assertEquals(PEOPLE_SCHEMA, scope.getSchema(PEOPLE_TABLE));
        Assert.assertEquals(SAMPLE_SCHEMA0, scope.getSchema(SAMPLE_TABLE0));
        Map<String,Schema> declaredSchemas = new HashMap<>();
        declaredSchemas.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        declaredSchemas.put(SAMPLE_TABLE0, SAMPLE_SCHEMA0);
        declaredSchemas.put(statmentId0, selectExtractStatement0OutputSchema);
        declaredSchemas.put(statmentId1, selectExtractStatement1OutputSchema);
        Assert.assertEquals(declaredSchemas, scope.getSchemas());
        // Assert added Statements
        Set<Statement> declaredStatements = new HashSet<>();
        declaredStatements.add(selectExtractStatement0);
        declaredStatements.add(selectExtractStatement1);
        Assert.assertEquals(declaredStatements, new HashSet<>(scope.getStatements()));
    }

    /**
     * Test the Scope by adding a Statement that require an non existing 
     * Schema. A TextDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithoutRelationManager03() throws TextDBException, StorageException {
        // Scope Initialization
        Scope scope = new Scope();
        
        // Build and add a statement into the scope
        String statementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
    }
    
    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Statement with an invalid Id (null) is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithoutRelationManager04() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);

        // Build and add a statement into the scope
        String statementId = null;
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, null, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
    }

    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Statement with an invalid Id (empty) is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithoutRelationManager05() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);

        // Build and add a statement into the scope
        String statementId = "";
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, null, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
    }
    
    /**
     * Test the behavior of the Scope (without using a RelationManager) when a
     * Statement with an already associated Id is inserted. A TextDBException
     * exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithoutRelationManager06() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE);
        Scope scope = new Scope(relationManager);

        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());

        // Build and add a statement into the scope
        String statementId = ALL_FIELD_TYPES_TABLE;
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
        scope.addStatement(selectExtractStatement);
    }    
    
    /**
     * Test the Scope with a RelationManager by adding a Statement,
     * retrieving it and retrieve its generated Schema.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddStatementWithRelationManager00() throws TextDBException, StorageException {
        /*
         * TODO: All implemented statements (CreateViewStatement and SelectExtractStatement), at the moment, have
         * another statement as dependency, thus no values can be added into the scope with only Statements.
         * This test Must be implemented in the future when a Statement can generate a schema by itself. 
         */
    }
    
    /**
     * Test the Scope with a RelationManager by adding a Statement that
     * require an added Schema, retrieving it and retrieving its generated
     * Schema.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test
    public void testAddStatementhWitRelationManager01() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(SAMPLE_TABLE0);
        Scope scope = new Scope(relationManager);
        
        // Add a schema into the scope
        scope.addSchema(PEOPLE_TABLE, PEOPLE_SCHEMA);
        // Build and add a statement into the scope
        String statementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        Schema selectExtractStatementOutputSchema = selectExtractStatement.generateOutputSchema(PEOPLE_SCHEMA);
        scope.addStatement(selectExtractStatement);
        
        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        
        // Assert added Schemas
        Assert.assertEquals(PEOPLE_SCHEMA, scope.getSchema(PEOPLE_TABLE));
        Assert.assertEquals(selectExtractStatementOutputSchema, scope.getSchema(statementId));
        Map<String,Schema> declaredSchemas = new HashMap<>();
        declaredSchemas.put(PEOPLE_TABLE, PEOPLE_SCHEMA);
        declaredSchemas.put(statementId, selectExtractStatementOutputSchema);
        Assert.assertEquals(declaredSchemas, scope.getSchemas());
        // Assert added Statements
        Set<Statement> declaredStatements = Collections.singleton(selectExtractStatement);
        Assert.assertEquals(declaredStatements, new HashSet<>(scope.getStatements()));
    }
        
    /**
     * Test the behavior of the Scope with a RelationManager when a Statement
     * is added with the same name of an existing table in the RelationManager
     * (when the table information has not yet been retrieved for). A 
     * extDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithRelationManager02() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE, ALL_FIELD_TYPES_TABLE);
        Scope scope = new Scope(relationManager);

        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());

        // Build and add statements into the scope
        String selectExtractStatementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(selectExtractStatementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
        String createviewStatementId = PEOPLE_TABLE;
        CreateViewStatement createviewStatement = new CreateViewStatement(createviewStatementId, selectExtractStatement);
        scope.addStatement(createviewStatement);
    }
    
    /**
     * Test the behavior of the Scope with a RelationManager when a Statement
     * is added with the same name of an existing table in the RelationManager
     * (when the table information has already been retrieved for). A
     * TextDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithRelationManager03() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables(PEOPLE_TABLE, ALL_FIELD_TYPES_TABLE);
        Scope scope = new Scope(relationManager);

        // Assert the value of the relation manager
        Assert.assertEquals(relationManager, scope.getRealationManager());
        
        // Make the Scope fetch a table schema and keep it in the collection of schemas 
        Assert.assertEquals(Utils.getSchemaWithID(ALL_FIELD_TYPES_SCHEMA), scope.getSchema(ALL_FIELD_TYPES_TABLE));
        
        // Build and add statements into the scope
        String selectExtractStatementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(selectExtractStatementId, selectPredicate, null, PEOPLE_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
        String createviewStatementId = PEOPLE_TABLE;
        CreateViewStatement createviewStatement = new CreateViewStatement(createviewStatementId, selectExtractStatement);
        scope.addStatement(createviewStatement);
    }

    /**
     * Test the behavior of the Scope with a RelationManager when a 
     * Statement require a non existing schema or table. 
     * A TextDBException exception is expected to be thrown.
     * @throws TextDBException If a TextDBException is thrown by the Scope.
     * @throws StorageException If an error occur in the RelationManager.
     */
    @Test(expected = TextDBException.class)
    public void testAddStatementWithRelationManager04() throws TextDBException, StorageException {
        // Scope and Tables Initialization
        initTestTables();
        Scope scope = new Scope(relationManager);

        // Build and add a statement into the scope
        String statementId = "_sid0";
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement(statementId, selectPredicate, null, ALL_FIELD_TYPES_TABLE, null, null);
        scope.addStatement(selectExtractStatement);
    }
    
}
