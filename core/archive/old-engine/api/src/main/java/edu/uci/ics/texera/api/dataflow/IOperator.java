package edu.uci.ics.texera.api.dataflow;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * Created by chenli on 3/25/16.
 */
public interface IOperator {

    static final int CLOSED = -1;
    static final int OPENED = 0;

    void open() throws TexeraException;

    Tuple getNextTuple() throws TexeraException;

    void close() throws TexeraException;

    Schema getOutputSchema();

    Schema transformToOutputSchema(Schema... inputSchema);
}
