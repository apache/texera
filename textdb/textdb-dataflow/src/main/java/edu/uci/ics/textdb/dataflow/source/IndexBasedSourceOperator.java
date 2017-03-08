package edu.uci.ics.textdb.dataflow.source;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * Created by chenli on 3/28/16.
 */
public class IndexBasedSourceOperator implements ISourceOperator {

    private DataReader dataReader;
    
    private int cursor = CLOSED;

    public IndexBasedSourceOperator(String tableName, Query query) throws DataFlowException {
        try {
            this.dataReader = RelationManager.getRelationManager().getTableDataReader(tableName, query);
        } catch (StorageException e) {
            throw new DataFlowException(e);
        }
    }

    @Override
    public void open() throws TextDBException {
        try {
            dataReader.open();
            cursor = OPENED;
        } catch (TextDBException e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            return dataReader.getNextTuple();
        } catch (TextDBException e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws TextDBException {
        try {
            dataReader.close();
            cursor = CLOSED;
        } catch (TextDBException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return dataReader.getOutputSchema();
    }

}
