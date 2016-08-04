package edu.uci.ics.textdb.dataflow.sink;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * IndexSink is a sink that writes tuples into an index.
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {
	
	private DataWriter dataWriter;
	
	public IndexSink(IOperator inputOperator, String indexDirectory, Schema schema, Analyzer luceneAnalyzer) {
		super(inputOperator);
		DataStore dataStore = new DataStore(indexDirectory, schema);
		this.dataWriter = new DataWriter(dataStore, luceneAnalyzer);
	}
	
	public IndexSink(String indexDirectory, Schema schema, Analyzer luceneAnalyzer) {
		super(null);
		DataStore dataStore = new DataStore(indexDirectory, schema);
		this.dataWriter = new DataWriter(dataStore, luceneAnalyzer);
	}
	
	public void open() throws Exception {
		super.open();
		this.dataWriter.clearData();
		this.dataWriter.open();
	}

    protected void processOneTuple(ITuple nextTuple) throws StorageException {
    	dataWriter.writeTuple(nextTuple);
    }
    
	public void close() throws Exception {
		if (this.dataWriter != null) {
			this.dataWriter.close();
		}
		super.close();
	}

}
