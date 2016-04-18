package edu.uci.ics.textdb.api.storage;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;


/**
 * 
 * @author sandeepreddy602
 *
 */
public interface IDataStore {
    public void incrementNumDocuments(int incrementBy);
    public int getNumDocuments();
    public String getDataDirectory();
    public List<Attribute> getSchema();
}
