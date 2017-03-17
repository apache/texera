package edu.uci.ics.textdb.api.dataflow;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * Created by chenli on 3/25/16.
 */
public interface IOperator {

    static final int CLOSED = -1;
    static final int OPENED = 0;

    void open() throws TextDBException;

    Tuple getNextTuple() throws TextDBException;

    void close() throws TextDBException;

    Schema getOutputSchema();
}
