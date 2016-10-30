package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.ITuple;

public interface IDataWriter {
    
    public void insertTuple(ITuple tuple) throws Exception;

    public void clearData() throws Exception;
}
