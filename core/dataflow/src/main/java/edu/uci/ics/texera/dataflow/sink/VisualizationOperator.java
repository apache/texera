package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;


/**
 * Base class for visualization operators
 * Author: Mingji Han
 */
public abstract class VisualizationOperator implements ITupleSink {

    protected IOperator inputOperator;
    protected int cursor = CLOSED;
    protected Schema outputSchema;
    protected List<Tuple> result = new ArrayList<>();
    protected final String type;

    public VisualizationOperator(String type) {
        this.type = type;
    }

    
    public void setInputOperator(IOperator inputOperator) {
        if (cursor != CLOSED) {
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_CHANGED_AFTER_OPEN);
        }
        this.inputOperator = inputOperator;
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
    public Tuple getNextTuple() throws TexeraException {
        return inputOperator.getNextTuple();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public List<Tuple> collectAllTuples() {
        processTuples();
        return result;
    }

    @Override
    public Schema transformToOutputSchema(Schema... inputSchema) {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }

    public String getChartType() {
        return type;
    }

    public static double extractNumber(IField field) {
        if (field instanceof DoubleField) {
            DoubleField doubleField = (DoubleField)field;
            return doubleField.getValue();
        }
        else if (field instanceof IntegerField) {
            IntegerField integerField = (IntegerField)field;
            return integerField.getValue().doubleValue();
        }
        throw new TexeraException("Invalid field type for extracting a number.");
    }

}
