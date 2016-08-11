package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.reader.DataReader;

/**
 * Created by chenli on 3/28/16.
 */
public class IndexBasedSourceOperator implements ISourceOperator {

	private IDataReader dataReader;
	private DataReaderPredicate predicate;
	private int cursor = CLOSED;
	
	public IndexBasedSourceOperator(DataReaderPredicate predicate){
	    this.predicate = predicate;
	}

	@Override
	public void open() throws DataFlowException {
		try {
		    dataReader = new DataReader(predicate);
			dataReader.open();
			cursor = OPENED;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	@Override
	public ITuple getNextTuple() throws DataFlowException {
	    if(cursor == CLOSED){
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
	public void close() throws DataFlowException {
		try {
			dataReader.close();
			cursor = CLOSED;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}
	
	/**
	 * Resets the predicate and resets the cursor. 
	 * The caller needs to reopen the operator once the predicate is reset.
	 * @param predicate
	 */
	public void resetPredicate(IPredicate predicate){
	    this.predicate = (DataReaderPredicate)predicate;
	    cursor = CLOSED;
	}

    @Override
    public Schema getOutputSchema() {
        return dataReader.getOutputSchema();
    }


}
