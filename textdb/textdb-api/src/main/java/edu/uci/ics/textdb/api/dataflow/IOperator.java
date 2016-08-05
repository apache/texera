package edu.uci.ics.textdb.api.dataflow;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

/**
 * Created by chenli on 3/25/16.
 */
public interface IOperator {
    
    static final int CLOSED = -1;
    static final int OPENED = 0;

    void open() throws Exception;

    ITuple getNextTuple() throws Exception;

    void close() throws Exception;
    
    Schema getOutputSchema();
}
