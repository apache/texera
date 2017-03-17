package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {

    private DataReader dataReader;
    
    private boolean isOpen = false;

    public ScanBasedSourceOperator(String tableName) throws DataFlowException {
        try {
            this.dataReader = RelationManager.getRelationManager().getTableDataReader(tableName, new MatchAllDocsQuery());
            // TODO add an option to set if payload is added in the future.
            this.dataReader.setPayloadAdded(true);
        } catch (StorageException e) {
            throw new DataFlowException(e);
        }
    }

    @Override
    public void open() throws TextDBException {
        if (isOpen) {
            return;
        }
        try {
            dataReader.open();
            isOpen = true;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (! isOpen) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            return dataReader.getNextTuple();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws TextDBException {
        if (! isOpen) {
            return;
        }
        try {
            dataReader.close();
            isOpen = false;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return dataReader.getOutputSchema();
    }
}
