package edu.uci.ics.textdb.storage;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.storage.IDataStore;

public class LuceneDataStore implements IDataStore{
    private String dataDirectory;
    private int numDocuments;
    private List<Attribute> schema;
    
    public LuceneDataStore(String dataDirectory, List<Attribute> schema){
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
    
    public List<Attribute> getSchema() {
        return schema;
    }
    
}
