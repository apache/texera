package edu.uci.ics.texera.storage;

import java.nio.file.Path;
import java.nio.file.Paths;

import edu.uci.ics.texera.api.schema.Schema;

public class DataStore {
    private Path dataDirectory;
    private int numDocuments;
    private Schema schema;

    public DataStore(String dataDirectory, Schema schema) {
        this.dataDirectory = Paths.get(dataDirectory);
        this.schema = schema;
    }
    
    public DataStore(Path dataDirectory, Schema schema) {
        this.dataDirectory = dataDirectory;
        this.schema = schema;
    }

    public void incrementNumDocuments(int incrementBy) {
        numDocuments += incrementBy;
    }

    public int getNumDocuments() {
        return numDocuments;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public Schema getSchema() {
        return schema;
    }

}
