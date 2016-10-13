package edu.uci.ics.textdb.storage.catalog;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.Attribute;
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

    public static void putDocumentSchema(String documentName, String indexDirectory, Schema documentSchema,
            String luceneAnalyzer) throws DataFlowException, StorageException {
        List<ITuple> documentCatalogData = new ArrayList<>();
        List<ITuple> schemaCatalogData = new ArrayList<>();

        if (!isCatalogManagerExist()) {
            documentCatalogData.addAll(CatalogConstants.getInitialDocumentCatalogTuples());
            schemaCatalogData.addAll(CatalogConstants.getInitialSchemaCatalogTuples());
        } else {
            documentCatalogData.addAll(readDocumentCatalog());
            schemaCatalogData.addAll(readSchemaCatalog());
        }

        if (findDocumentTuple(documentCatalogData, documentName) != null) {
            throw new StorageException(String.format("Document %s already exists", documentName));
        }

        ITuple documentTuple = new DataTuple(CatalogConstants.CATALOG_DOCUMENT_SCHEMA, new StringField(documentName),
                new StringField(indexDirectory), new StringField(luceneAnalyzer));
        documentCatalogData.add(documentTuple);

        for (int i = 0; i < documentSchema.getAttributes().size(); i++) {
            Attribute attr = documentSchema.getAttributes().get(i);
            ITuple schemaTuple = new DataTuple(CatalogConstants.CATALOG_SCHEMA_SCHEMA, new StringField(documentName),
                    new StringField(attr.getFieldName()), new StringField(attr.getFieldType().toString()),
                    new IntegerField(i));
            schemaCatalogData.add(schemaTuple);
        }

        writeDocumentCatalog(documentCatalogData);
        writeSchemaCatalog(schemaCatalogData);
    }

    public static String getDocumentDirectory(String documentName) throws StorageException, DataFlowException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }
        List<ITuple> documentCatalogData = readDocumentCatalog();

        ITuple documentTuple;
        if ((documentTuple = findDocumentTuple(documentCatalogData, documentName)) == null) {
            throw new StorageException(String.format("Document %s does not exist", documentName));
        }

        IField directoryField = documentTuple.getField(CatalogConstants.DOCUMENT_DIRECTORY);
        return directoryField.getValue().toString();
    }

    public static String getDocumentLuceneAnalyzer(String documentName) throws StorageException, DataFlowException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }
        List<ITuple> documentCatalogData = readDocumentCatalog();

        ITuple documentTuple;
        if ((documentTuple = findDocumentTuple(documentCatalogData, documentName)) == null) {
            throw new StorageException(String.format("Document %s does not exist", documentName));
        }

        IField directoryField = documentTuple.getField(CatalogConstants.DOCUMENT_LUCENE_ANALYZER);
        return directoryField.getValue().toString();
    }

    public static Schema getDocumentSchema(String documentName) throws StorageException, DataFlowException {
        if (!isCatalogManagerExist()) {
            throw new StorageException("Catalog Files not found");
        }

        List<ITuple> schemaCatalogData = readSchemaCatalog();
        Stream<ITuple> documentSchemaData = schemaCatalogData.stream()
                .filter(tuple -> documentName.toLowerCase()
                        .equals(tuple.getField(CatalogConstants.DOCUMENT_NAME).getValue().toString().toLowerCase()))
                .sorted((tuple1, tuple2) -> (((int) tuple1.getField(CatalogConstants.ATTR_POSITION)
                        .getValue()) < ((int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()) ? 0 : 1));

        return null;
    }

    public static boolean isCatalogManagerExist() {
        File documentCatalogFile = new File(CatalogConstants.DOCUMENT_CATALOG_DIRECTORY);
        File schemaCatalogFile = new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
        return documentCatalogFile.exists() && schemaCatalogFile.exists();
    }

    private static ITuple findDocumentTuple(List<ITuple> documentCatalogData, String documentName) {
        ITuple documentTuple = documentCatalogData.stream()
                .filter(tuple -> documentName.toLowerCase()
                        .equals(tuple.getField(CatalogConstants.DOCUMENT_NAME).getValue().toString().toLowerCase()))
                .findAny().orElse(null);
        return documentTuple;
    }

    private static List<ITuple> readDocumentCatalog() throws DataFlowException {
        DataStore documentCatalogStore = new DataStore(CatalogConstants.DOCUMENT_CATALOG_DIRECTORY,
                CatalogConstants.CATALOG_DOCUMENT_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), DataConstants.SCAN_QUERY,
                documentCatalogStore, documentCatalogStore.getSchema().getAttributes(),
                DataConstants.getStandardAnalyzer());
        DataReader documentCatalogReader = new DataReader(scanPredicate);

        List<ITuple> documentCatalogData = new ArrayList<>();
        ITuple tuple;
        while ((tuple = documentCatalogReader.getNextTuple()) != null) {
            documentCatalogData.add(tuple);
        }
        return documentCatalogData;
    }

    private static List<ITuple> readSchemaCatalog() throws DataFlowException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.CATALOG_SCHEMA_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), DataConstants.SCAN_QUERY,
                schemaCatalogStore, schemaCatalogStore.getSchema().getAttributes(),
                DataConstants.getStandardAnalyzer());
        DataReader schemaCatalogReader = new DataReader(scanPredicate);

        List<ITuple> documentCatalogData = new ArrayList<>();
        ITuple tuple;
        while ((tuple = schemaCatalogReader.getNextTuple()) != null) {
            documentCatalogData.add(tuple);
        }
        return documentCatalogData;
    }

    private static void writeDocumentCatalog(List<ITuple> documentCatalogTuples) throws StorageException {
        DataStore documentCatalogStore = new DataStore(CatalogConstants.DOCUMENT_CATALOG_DIRECTORY,
                CatalogConstants.CATALOG_DOCUMENT_SCHEMA);
        DataWriter documentCatalogWriter = new DataWriter(documentCatalogStore, DataConstants.getStandardAnalyzer());
        documentCatalogWriter.clearData();
        documentCatalogWriter.writeData(documentCatalogTuples);
    }

    private static void writeSchemaCatalog(List<ITuple> schemaCatalogTuples) throws StorageException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.CATALOG_SCHEMA_SCHEMA);
        DataWriter schemaCatalogWriter = new DataWriter(schemaCatalogStore, DataConstants.getStandardAnalyzer());
        schemaCatalogWriter.clearData();
        schemaCatalogWriter.writeData(schemaCatalogTuples);
    }

}
