package edu.uci.ics.textdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.common.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.common.AttributeType;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.utils.Utils;

public class RelationManager {
    
    private static volatile RelationManager singletonRelationManager = null;
    
    private RelationManager() throws StorageException {
        if (! checkCatalogExistence()) {
            initializeCatalog();
        }
    }

    public static RelationManager getRelationManager() throws StorageException {
        if (singletonRelationManager == null) {
            synchronized (RelationManager.class) {
                if (singletonRelationManager == null) {
                    singletonRelationManager = new RelationManager();
                }
            }
        }
        return singletonRelationManager;
    }

    /**
     * Checks if a table exists by looking it up in the catalog 
     *   and checking the folder in file system.
     * 
     * @param tableName
     * @return
     */
    public boolean checkTableExistence(String tableName) {
        try {
            String tableDirectory = getTableDirectory(tableName);
            getTableSchema(tableName);
            return DataReader.checkIndexExistence(tableDirectory);
        } catch (StorageException e) {
            return false;
        }
    }

    /**
     * Creates a new table. 
     *   Table name must be unique.
     *   LuceneAnalyzer must be a valid analyzer string.
     * 
     * The "_id" attribute will be added to the table schema.
     * System automatically generates a unique ID for each tuple inserted to a table,
     *   the generated ID will be in "_id" field.
     * 
     * @param tableName
     * @param indexDirectory
     * @param schema
     * @param luceneAnalyzerString
     * @throws StorageException
     */
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzerString)
            throws StorageException {
        // table should not exist
        if (checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s already exists.", tableName));
        }

        // check if the lucene analyzer string is valid
        Analyzer luceneAnalyzer = null;
        try {
            luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(luceneAnalyzerString);
        } catch (DataFlowException e) {
            throw new StorageException("Lucene Analyzer String is not valid.");
        }
        
        // clear all data in the index directory
        Schema tableSchema = Utils.getSchemaWithID(schema);
        DataStore tableDataStore = new DataStore(indexDirectory, tableSchema);
        DataWriter dataWriter = new DataWriter(tableDataStore, luceneAnalyzer);
        dataWriter.open();
        dataWriter.clearData();
        dataWriter.close();
        
        // write table info to catalog
        writeTableInfoToCatalog(tableName, indexDirectory, schema, luceneAnalyzerString);

    }

    /**
     * Deletes a table by its name.
     * If the table doesn't exist, it won't do anything.
     * Deleting system catalog tables is prohibited.
     * 
     * @param tableName
     * @throws StorageException
     */
    public void deleteTable(String tableName) throws StorageException {
        // User can't delete catalog table
        if (isSystemCatalog(tableName)) {
            throw new StorageException("Deleting a system catalog table is prohibited.");
        }
        // if table doesn't exist, then do nothing
        if (! checkTableExistence(tableName)) {
            return;
        }
        
        // try to clear all data in the table
        DataWriter dataWriter = new DataWriter(getTableDataStore(tableName), getTableAnalyzer(tableName));
        dataWriter.open();
        dataWriter.clearData();
        dataWriter.close();
        Utils.deleteDirectory(getTableDirectory(tableName));


        // generate a query for the table name
        Query catalogTableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));

        // delete the table from table catalog
        DataWriter tableCatalogWriter = new DataWriter(CatalogConstants.TABLE_CATALOG_DATASTORE, 
                LuceneAnalyzerConstants.getStandardAnalyzer());
        tableCatalogWriter.open();
        tableCatalogWriter.deleteTuple(catalogTableNameQuery);
        tableCatalogWriter.close();
        
        // delete the table from schema catalog
        DataWriter schemaCatalogWriter = new DataWriter(CatalogConstants.SCHEMA_CATALOG_DATASTORE,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        schemaCatalogWriter.open();
        schemaCatalogWriter.deleteTuple(catalogTableNameQuery);
        schemaCatalogWriter.close();
    }
    
    /**
     * Gets a tuple in a table by its _id field.
     * Returns null if the tuple doesn't exist.
     * 
     * @param tableName
     * @param idValue
     * @return
     * @throws StorageException
     */
    public Tuple getTupleByID(String tableName, IDField idField) throws StorageException {
        // construct the ID query
        Query tupleIDQuery = new TermQuery(new Term(SchemaConstants._ID, idField.getValue().toString()));
        
        // find the tuple using DataReader
        DataReader dataReader = getTableDataReader(tableName, tupleIDQuery);
        dataReader.setPayloadAdded(false);

        dataReader.open(); 
        Tuple tuple = dataReader.getNextTuple();
        dataReader.close();

        return tuple;
    }
    
    /**
     * Gets the DataWriter of a table. 
     * The DataWriter can be used to insert/delete/update tuples in a table.
     * 
     * @param tableName
     * @return
     * @throws StorageException
     */
    public DataWriter getTableDataWriter(String tableName) throws StorageException {
        if (isSystemCatalog(tableName)) {
            throw new StorageException("modify system catalog is not allowed");
        }
        return new DataWriter(getTableDataStore(tableName), getTableAnalyzer(tableName));
    }
    
    /**
     * Gets a DataReader for a table based on a query.
     * DataReader can return tuples that match the query.
     * 
     * @param tableName
     * @param tupleQuery
     * @return
     * @throws StorageException
     */
    public DataReader getTableDataReader(String tableName, Query tupleQuery) throws StorageException {
        DataStore tableDataStore = getTableDataStore(tableName);
        return new DataReader(tableDataStore, tupleQuery);
    }
    
    /**
     * Gets the DataStore(directory and schema) of a table.
     * 
     * @param tableName
     * @return
     * @throws StorageException
     */
    public DataStore getTableDataStore(String tableName) throws StorageException {
        String tableDirectory = getTableDirectory(tableName);
        Schema tableSchema = getTableSchema(tableName);
        return new DataStore(tableDirectory, tableSchema);
    }

    /**
     * Gets the directory of a table.
     * 
     * @param tableName
     * @return
     * @throws StorageException
     */
    public String getTableDirectory(String tableName) throws StorageException {
        // get the tuples with tableName from the table catalog
        Query tableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));
        DataReader tableCatalogDataReader = new DataReader(CatalogConstants.TABLE_CATALOG_DATASTORE, tableNameQuery);
        tableCatalogDataReader.setPayloadAdded(false);
        
        tableCatalogDataReader.open();
        Tuple nextTuple = tableCatalogDataReader.getNextTuple();
        tableCatalogDataReader.close();
        
        // if the tuple is not found, then the table name is not found
        if (nextTuple == null) {
            throw new StorageException(String.format("The directory for table %s is not found.", tableName));
        }
        
        // get the directory field
        IField directoryField = nextTuple.getField(CatalogConstants.TABLE_DIRECTORY);
        return directoryField.getValue().toString();
    }

    /**
     * Gets the schema of a table.
     * 
     * @param tableName
     * @return
     * @throws StorageException
     */
    public Schema getTableSchema(String tableName) throws StorageException {
        // get the tuples with tableName from the schema catalog
        Query tableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));
        DataReader schemaCatalogDataReader = new DataReader(CatalogConstants.SCHEMA_CATALOG_DATASTORE, tableNameQuery);
        
        
        // read the tuples into a list
        schemaCatalogDataReader.open();    
        List<Tuple> tableAttributeTuples = new ArrayList<>();
        Tuple nextTuple;
        while ((nextTuple = schemaCatalogDataReader.getNextTuple()) != null) {
            tableAttributeTuples.add(nextTuple);
        }

        // if the list is empty, then the schema is not found
        if (tableAttributeTuples.isEmpty()) {
            throw new StorageException(String.format("The schema of table %s is not found.", tableName));
        }
        
        // convert the unordered list of tuples to an order list of attributes
        List<Attribute> tableSchemaData = tableAttributeTuples.stream()
                // sort the tuples based on the attributePosition field.
                .sorted((tuple1, tuple2) -> Integer.compare((int) tuple1.getField(CatalogConstants.ATTR_POSITION).getValue(), 
                        (int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()))
                // map one tuple to one attribute
                .map(tuple -> new Attribute(tuple.getField(CatalogConstants.ATTR_NAME).getValue().toString(),
                        convertAttributeType(tuple.getField(CatalogConstants.ATTR_TYPE).getValue().toString())))
                .collect(Collectors.toList());
        
        return new Schema(tableSchemaData.stream().toArray(Attribute[]::new));
    }

    /**
     * Gets the Lucene analyzer of a table.
     *   
     * @param tableName
     * @return
     * @throws StorageException
     */
    public Analyzer getTableAnalyzer(String tableName) throws StorageException {
        // get the tuples with tableName from the table catalog
        Query tableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));

        DataReader tableCatalogDataReader = new DataReader(CatalogConstants.TABLE_CATALOG_DATASTORE, tableNameQuery);
        tableCatalogDataReader.setPayloadAdded(false);
        
        tableCatalogDataReader.open();
        Tuple nextTuple = tableCatalogDataReader.getNextTuple();
        tableCatalogDataReader.close();
        
        // if the tuple is not found, then the table name is not found
        if (nextTuple == null) {
            throw new StorageException(String.format("The analyzer for table %s is not found.", tableName));
        }
        
        // get the lucene analyzer string
        IField analyzerField = nextTuple.getField(CatalogConstants.TABLE_LUCENE_ANALYZER);
        String analyzerString = analyzerField.getValue().toString();
        
        // convert a lucene analyzer string to an analyzer object
        Analyzer luceneAnalyzer = null;
        try {
            luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(analyzerString);
        } catch (DataFlowException e) {
            throw new StorageException(e);
        }
        
        return luceneAnalyzer;
    }
    
    /*
     * This is a helper function that writes the table information to 
     *   the table catalog and the schema catalog.
     */
    private void writeTableInfoToCatalog(String tableName, String indexDirectory, Schema schema, String luceneAnalyzerString) 
            throws StorageException {   
        // write table catalog
        DataStore tableCatalogStore = new DataStore(CatalogConstants.TABLE_CATALOG_DIRECTORY,
                CatalogConstants.TABLE_CATALOG_SCHEMA);
        DataWriter dataWriter = new DataWriter(tableCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        dataWriter.open();
        dataWriter.insertTuple(CatalogConstants.getTableCatalogTuple(tableName, indexDirectory, luceneAnalyzerString));
        dataWriter.close();
       
        // write schema catalog
        Schema tableSchema = Utils.getSchemaWithID(schema);
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        dataWriter = new DataWriter(schemaCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        // each attribute in the table schema will be one row in schema catalog
        dataWriter.open();
        for (Tuple tuple : CatalogConstants.getSchemaCatalogTuples(tableName, tableSchema)) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }
    
    /*
     * This is a helper function to check if the system catalog tables exist physically on the disk.
     */
    private static boolean checkCatalogExistence() {
        return DataReader.checkIndexExistence(CatalogConstants.TABLE_CATALOG_DIRECTORY)
                && DataReader.checkIndexExistence(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
    }
    
    /*
     * This is a helper function to check if the table is a system catalog table.
     */
    private static boolean isSystemCatalog(String tableName) {
        return tableName.equals(CatalogConstants.TABLE_CATALOG) || tableName.equals(CatalogConstants.SCHEMA_CATALOG);
    }
    
    /*
     * Initializes the system catalog tables.
     */
    private void initializeCatalog() throws StorageException {
        // create table catalog
        writeTableInfoToCatalog(CatalogConstants.TABLE_CATALOG, 
                CatalogConstants.TABLE_CATALOG_DIRECTORY, 
                CatalogConstants.TABLE_CATALOG_SCHEMA,
                LuceneAnalyzerConstants.standardAnalyzerString());
        // create schema catalog
        writeTableInfoToCatalog(CatalogConstants.SCHEMA_CATALOG,
                CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA,
                LuceneAnalyzerConstants.standardAnalyzerString());
    }
    
    
    /*
     * Converts a attributeTypeString to AttributeType (case insensitive).
     * It returns null if string is not a valid type.
     * 
     */
    private static AttributeType convertAttributeType(String attributeTypeStr) {
        return Stream.of(AttributeType.values())
                .filter(typeStr -> typeStr.toString().toLowerCase().equals(attributeTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }
    
}
