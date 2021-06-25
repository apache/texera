package edu.uci.ics.texera.dataflow.sink.tuple;

import edu.uci.ics.texera.dataflow.sink.ITupleSink;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * TupleStreamSink is a sink that can be used by the caller to get tuples one by one.
 * 
 * @author Zuozhi Wang
 *
 */
public class TupleSink implements ITupleSink {
    private IOperator inputOperator;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private TupleSinkPredicate predicate;

    private Schema inputSchema;
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
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_CHANGED_AFTER_OPEN);
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
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema.Builder(inputSchema)
                .removeIfExists(SchemaConstants.PAYLOAD).build();
        cursor = OPENED;
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return ;
        }

        if (inputOperator != null)
            inputOperator.close();

        cursor = CLOSED;
    }

    @Override
    public void processTuples() throws TexeraException {
        return;
    }
    
    @Override
    public Tuple getNextTuple() throws TexeraException {
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
        return new Tuple.Builder(resultTuple)
                .removeIfExists(SchemaConstants.PAYLOAD).build();

    }

    /**
     * Collects ALL the tuples to an in-memory list.
     *
     * @return a list of tuples
     * @throws TexeraException
     */
    public List<Tuple> collectAllTuples() throws TexeraException {
        this.open();
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(tuple);
        }
        this.close();
        return results;
    }



    @Override
    public Schema transformToOutputSchema(Schema... inputSchema) {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }

}
