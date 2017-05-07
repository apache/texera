package edu.uci.ics.textdb.exp.sink.tuple;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

/**
 * TupleStreamSink is a sink that can be used by the caller to get tuples one by one.
 * 
 * @author Zuozhi Wang
 *
 */
public class TupleSink implements ISink {
    
    private TupleSinkPredicate predicate;
    
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
    private int cursor = CLOSED;

    /**
     * TupleStreamSink is a sink that can be used to
     *   collect tuples to an in-memory list.
     *
     * TupleStreamSink removes the payload attribute
     *   from the schema and each tuple.
     *
     */
    public TupleSink() {
        this(new TupleSinkPredicate());
    }
    
    public TupleSink(TupleSinkPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        if (cursor != CLOSED) {
            throw new RuntimeException(ErrorMessages.INPUT_OPERATOR_CHANGED_AFTER_OPEN);
        }
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new RuntimeException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = Utils.removeAttributeFromSchema(inputSchema, SchemaConstants.PAYLOAD);
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TextDBException {
        return;
    }
    
    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }
        if (cursor >= predicate.getLimit() + predicate.getOffset()) {
            return null;
        }
        Tuple resultTuple = null;
        while (true) {
            resultTuple = inputOperator.getNextTuple();
            if (resultTuple == null) {
                return null;
            }
            cursor++;
            if (cursor > predicate.getOffset()) {
                break;
            }
        }
        return Utils.removeFields(resultTuple, SchemaConstants.PAYLOAD);
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    /**
     * Collects ALL the tuples to an in-memory list.
     *
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
        this.open();
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(tuple);
        }
        this.close();
        return results;
    }

}
