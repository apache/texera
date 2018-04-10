package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * Created by chenli on 5/11/16.
 *
 * This abstract class leaves the @processOneTuple() function to be implemented
 * by the subclass based on the logic of handling each tuple coming from the
 * subtree.
 *
 */
public abstract class AbstractSink implements ISink {

    private IOperator inputOperator;
    private int cursor = CLOSED;

    /**
     * @about Opens the child operator.
     */
    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        inputOperator.open();
        cursor = OPENED;
    }

    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    @Override
    public void processTuples() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        Tuple nextTuple;

        while ((nextTuple = inputOperator.getNextTuple()) != null) {
            processOneTuple(nextTuple);
            cursor++;
        }
    }

    /**
     *
     * @param nextTuple
     *            A tuple that needs to be processed during each iteration
     */
    protected abstract void processOneTuple(Tuple nextTuple) throws TexeraException;

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        inputOperator.close();
        cursor = CLOSED;
    }
    
    public Schema getOutputSchema() {
        return this.inputOperator.getOutputSchema();
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
