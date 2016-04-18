package edu.uci.ics.textdb.api.storage;

import java.util.List;

import edu.uci.ics.textdb.api.common.ITuple;

public interface IDataWriter {
    public void writeData(List<ITuple> tuples) throws Exception;
    public void clearData() throws Exception;
}
