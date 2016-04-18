package edu.uci.ics.textdb.storage.writer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.utils.Utils;

public class LuceneDataWriter implements IDataWriter{
    
    private IDataStore dataStore;

    public LuceneDataWriter(IDataStore dataStore) {
        this.dataStore = dataStore;
    }
    
    @Override
    public void clearData() throws StorageException{
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(dataStore.getDataDirectory()));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, conf);
            indexWriter.deleteAll();
        } catch (Exception e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage(), e);
        } finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new StorageException(e.getMessage(), e);
                }
            }
        }
        
    }
    
    @Override
    public void writeData(List<ITuple> tuples) throws StorageException {
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(dataStore.getDataDirectory()));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, conf);
            
            for (ITuple sampleTuple : tuples) {

                Document document = getDocument(dataStore.getSchema(), sampleTuple);
                indexWriter.addDocument(document);
            }
            dataStore.incrementNumDocuments(tuples.size());

        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage(), e);
        } finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new StorageException(e.getMessage(), e);
                }
            }
        }
    }

    private Document getDocument(List<Attribute> schema, ITuple tuple) {
        List<IField> fields = tuple.getFields();
        Document doc = new Document();
        for (int count = 0; count < fields.size(); count++) {
            IField field = fields.get(count);
            Attribute attr = schema.get(count);
            FieldType fieldType = attr.getFieldType();
            doc.add(Utils.getLuceneField(fieldType, attr.getFieldName(), field.getValue()));
        }
        return doc;
    }

}
