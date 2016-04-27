package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * Created by chenli on 3/28/16.
 */
public class IndexSearchSourceOperator implements ISourceOperator {

	private IDataReader dataReader;

	public IndexSearchSourceOperator(IDataReader dataReader) throws DataFlowException {
		this.dataReader = dataReader;
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
}
