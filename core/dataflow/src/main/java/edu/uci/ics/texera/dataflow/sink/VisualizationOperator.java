package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class VisualizationOperator implements ISink, IVisualization {

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
    public Schema transformToOutputSchema(Schema... inputSchema) {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }


    public List<Tuple> collectAllTuples() {
        processTuples();
        return result;
    }



    public String getChartType() {
        return type;
    }

}
