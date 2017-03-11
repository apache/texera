package edu.uci.ics.textdb.dataflow.sink;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;

/**
 * TupleStreamSink is a sink that can be used by the caller to get tuples one by one.
 * 
 * @author Zuozhi Wang
 *
 */
public class TupleStreamSink implements ISink {
    
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
    private boolean isOpen = false;;

    /**
     * TupleStreamSink is a sink that can be used to
     *   collect tuples to an in-memory list.
     *
     * TupleStreamSink removes the _id attribute and payload attribute
     *   from the schema and each tuple.
     *
     */
    public TupleStreamSink() {
    }
    
    public void setInputOperator(IOperator inputOperator) {
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
        if (isOpen) {
            return;
        }     
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = Utils.removeAttributeFromSchema(inputSchema, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        isOpen = true;
    }

    @Override
    public void processTuples() throws TextDBException {
        return;
    }
    
    @Override
    public Tuple getNextTuple() throws TextDBException {
        Tuple tuple = inputOperator.getNextTuple();
        if (tuple == null) {
            return null;
        }
        return Utils.removeFields(tuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
    }

    @Override
    public void close() throws TextDBException {
        if (! isOpen) {
        }
        inputOperator.close();
        isOpen = false;        
    }

    /**
     * Collects ALL the tuples to an in-memory list.
     *
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            results.add(Utils.removeFields(tuple, SchemaConstants._ID, SchemaConstants.PAYLOAD));
        }
        return results;
    }

}
