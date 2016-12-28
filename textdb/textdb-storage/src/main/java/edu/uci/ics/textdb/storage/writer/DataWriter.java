package edu.uci.ics.textdb.storage.writer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.utils.Utils;

public class DataWriter implements IDataWriter {

    private IDataStore dataStore;
    private Analyzer analyzer;

    private IndexWriter luceneIndexWriter;

    public DataWriter(IDataStore dataStore, Analyzer analyzer) {
        this.dataStore = dataStore;
        this.analyzer = analyzer;
    }

    @Override
    public void clearData() throws StorageException { 
        if (this.luceneIndexWriter == null || ! this.luceneIndexWriter.isOpen()) {
            open();
        }
        try {
            this.luceneIndexWriter.deleteAll();
        } catch (IOException e) {
            throw new StorageException(e.getMessage(), e);
        } finally {
            close();
        }

    }

    public void open() throws StorageException {
        if (this.luceneIndexWriter == null || ! this.luceneIndexWriter.isOpen()) {
            try {
                Directory directory = FSDirectory.open(Paths.get(dataStore.getDataDirectory()));
                IndexWriterConfig conf = new IndexWriterConfig(analyzer);
                this.luceneIndexWriter = new IndexWriter(directory, conf);
            } catch (IOException e) {
                throw new StorageException(e.getMessage(), e);
            }
        }
    }

    public void close() throws StorageException {
        if (this.luceneIndexWriter != null) {
            try {
                this.luceneIndexWriter.close();
            } catch (IOException e) {
                throw new StorageException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void insertTuple(ITuple tuple) throws StorageException {
        if (this.luceneIndexWriter == null || ! this.luceneIndexWriter.isOpen()) {
            open();
        }
        try {
            Document document = getDocument(dataStore.getSchema(), tuple);
            this.luceneIndexWriter.addDocument(document);
            this.dataStore.incrementNumDocuments(1);
        } catch (IOException e) {
            close();
            throw new StorageException(e.getMessage(), e);
        } finally {

        }
    }
    
    public void deleteTuple(Query... luceneQuery) throws StorageException {
        if (this.luceneIndexWriter == null || ! this.luceneIndexWriter.isOpen()) {
            open();
        }
        try {
            this.luceneIndexWriter.deleteDocuments(luceneQuery);
        } catch (IOException e) {
            close();
            throw new StorageException(e.getMessage(), e);
        } finally {

        }
    }

    
    private Document getDocument(Schema schema, ITuple tuple) {
        List<IField> fields = tuple.getFields();
        List<Attribute> attributes = schema.getAttributes();
        Document doc = new Document();
        for (int count = 0; count < fields.size(); count++) {
            IField field = fields.get(count);
            Attribute attr = attributes.get(count);
            FieldType fieldType = attr.getFieldType();
            doc.add(Utils.getLuceneField(fieldType, attr.getFieldName(), field.getValue()));
        }
        return doc;
    }

}