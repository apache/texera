package edu.uci.ics.textdb.storage;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataStore;

public class LuceneDataStore implements IDataStore {
    private String dataDirectory;
    private int numDocuments;
    private Schema schema;

    public LuceneDataStore(String dataDirectory, Schema schema) {
        this.dataDirectory = dataDirectory;
        this.schema = schema;
    }

    @Override
    public void incrementNumDocuments(int incrementBy) {
        numDocuments += incrementBy;
    }

    @Override
    public int getNumDocuments() {
        return numDocuments;
    }

    @Override
    public String getDataDirectory() {
        return dataDirectory;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

}
