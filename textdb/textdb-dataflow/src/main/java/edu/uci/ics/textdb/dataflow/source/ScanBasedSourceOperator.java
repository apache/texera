package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {
    
    private IDataReader dataReader;
    private Schema schema;
    
    public ScanBasedSourceOperator(IDataReader dataReader) throws DataFlowException {
        this.dataReader = dataReader;
        this.schema = dataReader.getOutputSchema();
    }

    @Override
    public void open() throws DataFlowException {
        try {
            dataReader.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        try {
            return dataReader.getNextTuple();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws DataFlowException {
        try {
            dataReader.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
	public Schema getOutputSchema() {
		return this.schema;
	}    
}
