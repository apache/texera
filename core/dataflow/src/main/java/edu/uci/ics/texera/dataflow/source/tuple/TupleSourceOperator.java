package edu.uci.ics.texera.dataflow.source.tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/*
 * This operator takes a collection of tuples and serves them as a source operator.
 * 
 * If the "_id" field is not in the schema, this operator will add an "_id" field to the schema and tuples.
 * 
 * This operator is intended for internal use and testing purposes. 
 * It will NOT be exposed to the web API, therefore it doesn't have a corresponding predicate.
 * 
 */
public class TupleSourceOperator implements ISourceOperator {
    
    private Collection<Tuple> inputTuples;
    private Iterator<Tuple> tupleIterator;
    private Schema outputSchema;
    
    private int cursor = CLOSED;
    
    public TupleSourceOperator(Collection<Tuple> inputTuples, Schema schema) {
        this(inputTuples, schema, true);
    }
    
    /**
     * 
     * @param inputTuples, a collection of tuples as the source
     * @param schema, the schema corresponds to the input tuples
     * @param addIDAttribute, a flag that indicates if TupleSourceOperator automatically adds an _ID attribute or not
     */
    public TupleSourceOperator(Collection<Tuple> inputTuples, Schema schema, Boolean addIDAttribute) {
        if (addIDAttribute && ! schema.containsAttribute(SchemaConstants._ID)) {
            this.outputSchema = new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE).add(schema).build();
            this.inputTuples = new ArrayList<>();
            for (Tuple tuple : inputTuples) {
                List<IField> fieldsWithID = new ArrayList<>();
                fieldsWithID.add(IDField.newRandomID());
                fieldsWithID.addAll(tuple.getFields());
                this.inputTuples.add(new Tuple(outputSchema, fieldsWithID));
            }
        } else {
            this.outputSchema = schema;
            this.inputTuples = inputTuples;
        }
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        this.tupleIterator = this.inputTuples.iterator();
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (tupleIterator.hasNext()) {
            return tupleIterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        this.tupleIterator = null;
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema == null || inputSchema.length == 0)
            return getOutputSchema();
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
