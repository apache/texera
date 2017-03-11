package edu.uci.ics.textdb.storage;

import edu.uci.ics.textdb.api.schema.Schema;

public class DataStore {
    private String dataDirectory;
    private int numDocuments;
    private Schema schema;

    public DataStore(String dataDirectory, Schema schema) {
        this.dataDirectory = dataDirectory;
        this.schema = schema;
    }

    public void incrementNumDocuments(int incrementBy) {
        numDocuments += incrementBy;
    }

    public int getNumDocuments() {
        return numDocuments;
    }

    public String getDataDirectory() {
        return dataDirectory;
    }

    public Schema getSchema() {
        return schema;
    }

}
