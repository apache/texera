package edu.uci.ics.textdb.dataflow.source;

import java.util.Collection;
import java.util.Iterator;

import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/*
 * This operator takes an in-memory list of tuples and serves them as a source operator.
 * 
 */
public class TupleStreamSourceOperator implements ISourceOperator {
    
    private Collection<Tuple> tupleInputs;
    private Iterator<Tuple> tupleIterator;
    private Schema outputSchema;
    
    private boolean isOpen;
    
    public TupleStreamSourceOperator(Collection<Tuple> tupleInputs, Schema schema) {
        this.tupleInputs = tupleInputs;
        this.outputSchema = schema;
        this.tupleIterator = null;
        this.isOpen = false;
    }

    @Override
    public void open() throws TextDBException {
        if (isOpen) {
            return;
        }
        tupleIterator = tupleInputs.iterator();        
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (tupleIterator.hasNext()) {
            return tupleIterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() throws TextDBException {
        isOpen = false;        
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
}
