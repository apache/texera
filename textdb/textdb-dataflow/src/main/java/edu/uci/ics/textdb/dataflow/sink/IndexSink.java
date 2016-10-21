package edu.uci.ics.textdb.dataflow.sink;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.catalog.CatalogManager;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * IndexSink is a sink that writes tuples into an index.
 * 
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {

    private String collectionName;
    private String indexDirectory;
    private Schema collectionSchema;
    private Analyzer luceneAnalyzer;
    
    private DataWriter dataWriter;
    
    private boolean isAppend = false;
    

    public IndexSink(String collectionName, String indexDirectory, Schema schema, Analyzer luceneAnalyzer) {
        this.collectionName = collectionName;
        this.indexDirectory = indexDirectory;
        this.collectionSchema = schema;
        this.luceneAnalyzer = luceneAnalyzer;
        
        DataStore dataStore = new DataStore(indexDirectory, schema);
        this.dataWriter = new DataWriter(dataStore, luceneAnalyzer);
    }

    public void open() throws Exception {
        super.open();
        if (! isAppend) {
            this.dataWriter.clearData();
            if (! CatalogManager.isCatalogManagerExist()) {
                CatalogManager.createCatalog();
            }
            CatalogManager.putCollectionSchema(collectionName, indexDirectory, collectionSchema, luceneAnalyzer.toString());
        }
        this.dataWriter.open();
    }

    protected void processOneTuple(ITuple nextTuple) throws StorageException {
        dataWriter.writeTuple(nextTuple);
    }

    public void close() throws Exception {
        if (this.dataWriter != null) {
            this.dataWriter.close();
        }
        super.close();
    }

}
