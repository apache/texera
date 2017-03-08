package edu.uci.ics.textdb.dataflow.sink;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * IndexSink is a sink that writes tuples into an index.
 * 
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {

    private DataWriter dataWriter;
    private boolean isAppend = false;

    public IndexSink(String tableName, boolean isAppend) throws DataFlowException {
        try {
            RelationManager relationManager = RelationManager.getRelationManager();
            this.dataWriter = relationManager.getTableDataWriter(tableName);
            this.isAppend = isAppend;
        } catch (StorageException e) {
            throw new DataFlowException(e);
        }

    }

    public void open() throws TextDBException {
        super.open();
        this.dataWriter.open();
        if (! this.isAppend) {
            this.dataWriter.clearData();
        }
    }

    protected void processOneTuple(Tuple nextTuple) throws TextDBException {
        dataWriter.insertTuple(nextTuple);
    }

    public void close() throws TextDBException {
        if (this.dataWriter != null) {
            this.dataWriter.close();
        }
        super.close();
    }

}
