package edu.uci.ics.textdb.storage.catalog;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.*;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class CatalogManager {
    
    public static final String DOCUMENT_CATALOG = "documentCatalog";
    public static final String SCHEMA_CATALOG = "schemaCatalog";
    
    public static final String DOCUMENT_CATALOG_DIRECTORY = "../catalog/document";
    public static final String SCHEMA_CATALOG_DIRECTORY = "../catalog/schema";
    
    
    public static Schema getDocumentSchema(String documentName) throws StorageException {
        if (! isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }

        return null;
    }
    
    public static boolean isCatalogManagerExist() {
        File documentCatalogFile = new File(DOCUMENT_CATALOG_DIRECTORY);
        File schemaCatalogFile = new File(SCHEMA_CATALOG_DIRECTORY);
        return documentCatalogFile.exists() && schemaCatalogFile.exists();
    }
    
    public static void initializeCatalogSchema() throws StorageException {
        DataStore catalogDocumentStore = new DataStore(DOCUMENT_CATALOG_DIRECTORY, CatalogSchema.CATALOG_DOCUMENT_SCHEMA);
        DataWriter catalogDocumentWriter = new DataWriter(catalogDocumentStore, DataConstants.getStandardAnalyzer());
        catalogDocumentWriter.writeData(getInitialDocumentCatalogTuples());

        DataStore catalogSchemaStore = new DataStore(SCHEMA_CATALOG_DIRECTORY, CatalogSchema.CATALOG_SCHEMA_SCHEMA);
        DataWriter catalogSchemaWriter = new DataWriter(catalogSchemaStore, DataConstants.getStandardAnalyzer());
        catalogSchemaWriter.writeData(getInitialSchemaCatalogTuples());
    }
    
    private static List<ITuple> getInitialDocumentCatalogTuples() {
        IField[] documentFields = {
                new IntegerField(0), 
                new StringField(DOCUMENT_CATALOG),
                new StringField(DOCUMENT_CATALOG_DIRECTORY),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER)
        };
        IField[] schemaFields = {
                new IntegerField(1), 
                new StringField(SCHEMA_CATALOG),
                new StringField(SCHEMA_CATALOG_DIRECTORY),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER)
        };
        
        List<IField[]> fieldsList = Arrays.asList(documentFields, schemaFields);
        return fieldsList.stream().map(fields -> new DataTuple(CatalogSchema.CATALOG_DOCUMENT_SCHEMA, fields)).collect(Collectors.toList());
    }
    
    private static List<ITuple> getInitialSchemaCatalogTuples() {
        IField[] documentIDFields = {
                new IntegerField(0),
                new StringField(CatalogSchema.DOCUMENT_ID),
                new StringField("integer")
        };
        IField[] documentNameFields = {
                new IntegerField(0),
                new StringField(CatalogSchema.DOCUMENT_NAME),
                new StringField("string") 
        };
        IField[] documentDirectoryFields = {
                new IntegerField(0),
                new StringField(CatalogSchema.DOCUMENT_DIRECTORY),
                new StringField("string")
        };
        IField[] documentLuceneAnalyzerFields = {
                new IntegerField(0),
                new StringField(CatalogSchema.DOCUMENT_LUCENE_ANALYZER),
                new StringField("string")
        };
        IField[] schemaIDFields = {
                new IntegerField(1),
                new StringField(CatalogSchema.DOCUMENT_ID),
                new StringField("integer")
        };
        IField[] schemaAttrNameFields = {
                new IntegerField(1),
                new StringField(CatalogSchema.ATTR_NAME),
                new StringField("string")
        };
        IField[] schemaAttrTypeFields = {
                new IntegerField(1),
                new StringField(CatalogSchema.ATTR_TYPE),
                new StringField("string")
        };
        
        List<IField[]> fieldsList = Arrays.asList(documentIDFields, documentNameFields, documentDirectoryFields, 
                documentLuceneAnalyzerFields, schemaIDFields, schemaAttrNameFields, schemaAttrTypeFields);
        
        return fieldsList.stream().map(fields -> new DataTuple(CatalogSchema.CATALOG_SCHEMA_SCHEMA, fields)).collect(Collectors.toList());
    }
    
}
