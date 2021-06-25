package edu.uci.ics.texera.storage.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DateTimeField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.AttributeType;

public class StorageUtils {
    
    public static IField getField(AttributeType attributeType, String fieldValue) throws ParseException {
        IField field = null;
        switch (attributeType) {
        case _ID_TYPE:
            field = new IDField(fieldValue);
            break;
        case STRING:
            field = new StringField(fieldValue);
            break;
        case INTEGER:
            field = new IntegerField(Integer.parseInt(fieldValue));
            break;
        case DOUBLE:
            field = new DoubleField(Double.parseDouble(fieldValue));
            break;
        case DATE:
            field = new DateField(fieldValue);
            break;
        case DATETIME:
            field = new DateTimeField(fieldValue);
            break;
        case TEXT:
            field = new TextField(fieldValue);
            break;
        case LIST:
            // LIST FIELD SHOULD BE CREATED ON ITS OWN
            // WARNING! This case should never be reached.
            field = new ListField<String>(Arrays.asList(fieldValue));
            break;
        }
        return field;
    }

    public static IndexableField getLuceneField(AttributeType attributeType, String attributeName, Object fieldValue) {
        IndexableField luceneField = null;
        switch (attributeType) {
        // _ID_TYPE is currently same as STRING
        case _ID_TYPE:
        case STRING:
            luceneField = new org.apache.lucene.document.StringField(attributeName, (String) fieldValue, Store.YES);
            break;
        case INTEGER:
            luceneField = new org.apache.lucene.document.IntField(attributeName, (Integer) fieldValue, Store.YES);
            break;
        case DOUBLE:
            luceneField = new org.apache.lucene.document.DoubleField(attributeName, (Double) fieldValue, Store.YES);
            break;
        case DATE:
            String dateString = fieldValue.toString();
            luceneField = new org.apache.lucene.document.StringField(attributeName, dateString, Store.YES);
            break;
        case DATETIME:
            String dateTimeString = fieldValue.toString();
            luceneField = new org.apache.lucene.document.StringField(attributeName, dateTimeString, Store.YES);
            break;
        case TEXT:
            // By default we enable positional indexing in Lucene so that we can
            // return
            // information about character offsets and token offsets
            org.apache.lucene.document.FieldType luceneFieldType = new org.apache.lucene.document.FieldType();
            luceneFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            luceneFieldType.setStored(true);
            luceneFieldType.setStoreTermVectors(true);
            luceneFieldType.setStoreTermVectorOffsets(true);
            luceneFieldType.setStoreTermVectorPayloads(true);
            luceneFieldType.setStoreTermVectorPositions(true);
            luceneFieldType.setTokenized(true);

            luceneField = new org.apache.lucene.document.Field(attributeName, (String) fieldValue, luceneFieldType);

            break;
        case LIST:
            // Lucene doesn't have list field
            // WARNING! This case should never be reached.
            break;
        }
        return luceneField;
    }
    
    public static void deleteDirectory(String indexDir) throws StorageException {
        Path directory = Paths.get(indexDir);
        if (!Files.exists(directory)) {
            return;
        }

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new StorageException("failed to delete a given directory", e);
        }
    }

}
