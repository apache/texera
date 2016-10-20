package edu.uci.ics.textdb.storage.catalog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.*;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class CatalogManager {
    
    /**
     * @return true if the catalog manager exists
     */
    public static boolean isCatalogManagerExist() {
        File collectionCatalogFile = new File(CatalogConstants.COLLECTION_CATALOG_DIRECTORY);
        File schemaCatalogFile = new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
        return collectionCatalogFile.exists() && schemaCatalogFile.exists();
    }

    /**
     * Create the catalog.
     * 
     * @throws StorageException
     */
    public static void createCatalog() throws StorageException {
        System.out.println("creating catalog");
        List<ITuple> collectionCatalogData = CatalogConstants.getInitialCollectionCatalogTuples();
        List<ITuple> schemaCatalogData = CatalogConstants.getInitialSchemaCatalogTuples();
        
        writeCollectionCatalog(collectionCatalogData);
        writeSchemaCatalog(schemaCatalogData);
    }
    
    /**
     * Clear the content of the catalog.
     * 
     * @throws StorageException
     */
    public static void clearCatalog() throws StorageException {
        System.out.println("clearing catalog");
        writeCollectionCatalog(new ArrayList<ITuple>());
        writeSchemaCatalog(new ArrayList<ITuple>());
    }
   
    
    /**
     * Put a collecction's name, directory, lucene analyzer, and schema into the catalog.
     * 
     * @param collectionName, name of the collection
     * @param indexDirectory, directory of collection's index
     * @param collectionSchema, collection's schema
     * @param luceneAnalyzer, lucene analyzer that collection is using
     * @throws StorageException, if the collection name already exists
     */
    public static void putCollectionSchema(String collectionName, String indexDirectory, Schema collectionSchema,
            String luceneAnalyzer) throws StorageException {
        List<ITuple> collectionCatalogData = new ArrayList<>();
        List<ITuple> schemaCatalogData = new ArrayList<>();

        if (!isCatalogManagerExist()) {
            collectionCatalogData.addAll(CatalogConstants.getInitialCollectionCatalogTuples());
            schemaCatalogData.addAll(CatalogConstants.getInitialSchemaCatalogTuples());
        } else {
            collectionCatalogData.addAll(readCollectionCatalog());
            schemaCatalogData.addAll(readSchemaCatalog());
        }

        if (findCollectionData(collectionCatalogData, collectionName) != null) {
            throw new StorageException(String.format("Collection %s already exists", collectionName));
        }

        String indexDirectoryAbsolutePath;
        try {
            indexDirectoryAbsolutePath = new File(indexDirectory).getCanonicalPath();
        } catch (IOException e) {
            throw new StorageException(e.getMessage(), e);
        }
        ITuple collectionTuple = new DataTuple(CatalogConstants.COLLECTION_CATALOG_SCHEMA, new StringField(collectionName),
                new StringField(indexDirectoryAbsolutePath), new StringField(luceneAnalyzer));
        collectionCatalogData.add(collectionTuple);

        for (int i = 0; i < collectionSchema.getAttributes().size(); i++) {
            Attribute attr = collectionSchema.getAttributes().get(i);
            ITuple schemaTuple = new DataTuple(CatalogConstants.SCHEMA_CATALOG_SCHEMA, new StringField(collectionName),
                    new StringField(attr.getFieldName()), new StringField(attr.getFieldType().toString()),
                    new IntegerField(i));
            schemaCatalogData.add(schemaTuple);
        }

        writeCollectionCatalog(collectionCatalogData);
        writeSchemaCatalog(schemaCatalogData);
    }
    
    public static void deleteCollectionCatalog(String collectionName) throws StorageException {
        if (! isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }
        
        List<ITuple> collectionCatalogData = readCollectionCatalog();
        List<ITuple> schemaCatalogData = readSchemaCatalog();

        List<ITuple> newCollectionCatalogData = collectionCatalogData.stream()
                .filter(tuple -> ! isCollectionNameInTuple(tuple, collectionName)).collect(Collectors.toList());
        List<ITuple> newSchemaCatalogData = schemaCatalogData.stream()
                .filter(tuple -> ! isCollectionNameInTuple(tuple, collectionName))
                .collect(Collectors.toList());

        writeCollectionCatalog(newCollectionCatalogData);
        writeSchemaCatalog(newSchemaCatalogData);
    }
    
    private static boolean isCollectionNameInTuple(ITuple tuple, String collectionName) {
        return collectionName.toLowerCase().equals(
                tuple.getField(CatalogConstants.COLLECTION_NAME).getValue().toString().toLowerCase());
    }

    /**
     * Get the index directory of a collection by its name.
     * 
     * @param collectionName, the name of the collection
     * @return the index directory of the collection
     * @throws StorageException, if the collection doesn't exist, or the catalog doesn't exist.
     */
    public static String getCollectionDirectory(String collectionName) throws StorageException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }
        List<ITuple> collectionCatalogData;
        collectionCatalogData = readCollectionCatalog();

        ITuple collectionTuple;
        if ((collectionTuple = findCollectionData(collectionCatalogData, collectionName)) == null) {
            throw new StorageException(String.format("Collection %s does not exist", collectionName));
        }

        IField directoryField = collectionTuple.getField(CatalogConstants.COLLECTION_DIRECTORY);
        return directoryField.getValue().toString();
    }

    /**
     * Get the lucene analyzer that the collection is using by its name.
     * 
     * @param collectionName
     * @return a string representing its lucene analyzer
     * @throws StorageException, if the collection doesn't exist, or the catalog doesn't exist.
     */
    public static String getCollectionLuceneAnalyzer(String collectionName) throws StorageException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }
        List<ITuple> collectionCatalogData = readCollectionCatalog();

        ITuple collectionTuple;
        if ((collectionTuple = findCollectionData(collectionCatalogData, collectionName)) == null) {
            throw new StorageException(String.format("Collection %s does not exist", collectionName));
        }

        IField directoryField = collectionTuple.getField(CatalogConstants.COLLECTION_LUCENE_ANALYZER);
        return directoryField.getValue().toString();
    }

    /**
     * Get the schema of the collection by its name.
     * 
     * @param collectionName
     * @return collection's schema
     * @throws StorageException, if the collection doesn't exist, or the catalog doesn't exist.
     */
    public static Schema getCollectionSchema(String collectionName) throws StorageException, DataFlowException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }

        List<ITuple> schemaCatalogData = readSchemaCatalog();
        List<Attribute> collectionSchemaData = schemaCatalogData.stream()
                .filter(tuple -> collectionName.toLowerCase()
                        .equals(tuple.getField(CatalogConstants.COLLECTION_NAME).getValue().toString().toLowerCase()))
                .sorted((tuple1, tuple2) -> Integer.compare((int) tuple1.getField(CatalogConstants.ATTR_POSITION).getValue(), 
                        (int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()))
                .map(tuple -> new Attribute(tuple.getField(CatalogConstants.ATTR_NAME).getValue().toString(),
                        convertAttributeType(tuple.getField(CatalogConstants.ATTR_TYPE).getValue().toString())))
                .collect(Collectors.toList());
        
        if (collectionSchemaData.isEmpty()) {
            throw new StorageException(String.format("Collection %s doesn't exist", collectionName));
        }
        return new Schema(collectionSchemaData.stream().toArray(Attribute[]::new));
    }
    
    /**
     * This function converts a attributeTypeString to FieldType (case insensitive). 
     * It returns null if string is not a valid type.
     * 
     * @param attributeTypeStr
     * @return FieldType, null if attributeTypeStr is not a valid type.
     */
    private static FieldType convertAttributeType(String attributeTypeStr) {
        return Stream.of(FieldType.values())
                .filter(typeStr -> typeStr.toString().toLowerCase().equals(attributeTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }    


    private static ITuple findCollectionData(List<ITuple> collectionCatalogData, String collectionName) {
        ITuple collectionTuple = collectionCatalogData.stream()
                .filter(tuple -> collectionName.toLowerCase()
                        .equals(tuple.getField(CatalogConstants.COLLECTION_NAME).getValue().toString().toLowerCase()))
                .findAny().orElse(null);
        return collectionTuple;
    }

    /*
     * Read the collection catalog from catalog files.
     */
    private static List<ITuple> readCollectionCatalog() throws StorageException {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), DataConstants.SCAN_QUERY,
                collectionCatalogStore, collectionCatalogStore.getSchema().getAttributes(),
                DataConstants.getStandardAnalyzer());
        DataReader collectionCatalogReader = new DataReader(scanPredicate);
        collectionCatalogReader.setIsPayloadAdded(false);
        
        List<ITuple> collectionCatalogData = new ArrayList<>();
        try {
            collectionCatalogReader.open();
            ITuple tuple;
            while ((tuple = collectionCatalogReader.getNextTuple()) != null) {
                collectionCatalogData.add(tuple);
            }
            collectionCatalogReader.close();
        } catch (DataFlowException e) {
            throw new StorageException(e.getMessage(), e);
        }

        return collectionCatalogData;
    }

    /*
     * Read the schema catalog from catalog files.
     */
    private static List<ITuple> readSchemaCatalog() throws StorageException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), DataConstants.SCAN_QUERY,
                schemaCatalogStore, schemaCatalogStore.getSchema().getAttributes(),
                DataConstants.getStandardAnalyzer());
        DataReader schemaCatalogReader = new DataReader(scanPredicate);
        schemaCatalogReader.setIsPayloadAdded(false);

        List<ITuple> schemaCatalogData = new ArrayList<>();
        try {
            schemaCatalogReader.open();
            ITuple tuple;
            while ((tuple = schemaCatalogReader.getNextTuple()) != null) {
                schemaCatalogData.add(tuple);
            }
            schemaCatalogReader.close();
        } catch (DataFlowException e) {
            throw new StorageException(e.getMessage(), e);
        }

        return schemaCatalogData;
    }

    /*
     * Write the collection catalog to catalog files.
     * 
     * !!!This write operation will overwrite the old catalog.
     */
    private static void writeCollectionCatalog(List<ITuple> collectionCatalogTuples) throws StorageException {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataWriter collectionCatalogWriter = new DataWriter(collectionCatalogStore, DataConstants.getStandardAnalyzer());
        collectionCatalogWriter.clearData();
        collectionCatalogWriter.writeData(collectionCatalogTuples);
    }

    /*
     * Write the schema catalog to catalog files.
     * 
     * !!!This write operation will overwrite the old catalog.
     */
    private static void writeSchemaCatalog(List<ITuple> schemaCatalogTuples) throws StorageException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        DataWriter schemaCatalogWriter = new DataWriter(schemaCatalogStore, DataConstants.getStandardAnalyzer());
        schemaCatalogWriter.clearData();
        schemaCatalogWriter.writeData(schemaCatalogTuples);
    }

}
