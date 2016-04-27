package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.Schema;



/**
 * 
 * @author sandeepreddy602
 *
 */
public interface IDataStore {
    public void incrementNumDocuments(int incrementBy);
    public int getNumDocuments();
    public String getDataDirectory();
    public Schema getSchema();
}
