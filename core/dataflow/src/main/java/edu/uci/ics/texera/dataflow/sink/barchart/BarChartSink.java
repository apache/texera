package edu.uci.ics.texera.dataflow.sink.barchart;


import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.IVisualization;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BarChartSink extends VisualizationOperator {

    private BarChartSinkPredicate predicate;

    public BarChartSink(BarChartSinkPredicate predicate) {
        super(VisualizationConstants.BAR);
        this.predicate = predicate;
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

        Schema schema = inputOperator.getOutputSchema();

        Attribute nameColumn =  schema.getAttribute(predicate.getNameColumn());
        AttributeType nameColumnType = nameColumn.getType();
        if (!nameColumnType.equals(AttributeType.STRING) && !nameColumnType.equals(AttributeType.TEXT)) {
            throw new DataflowException("Type of name column should be string or text.");
        }

        Attribute dataColumn =  schema.getAttribute(predicate.getDataColumn());
        AttributeType dataColumnType = dataColumn.getType();
        if (!dataColumnType.equals(AttributeType.DOUBLE) && !dataColumnType.equals(AttributeType.INTEGER)) {
            throw new DataflowException(("Type of data column should be integer or double."));
        }

        outputSchema = new Schema.Builder().add(nameColumn, dataColumn).build();
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        List<Tuple> list = new ArrayList<>();

        Tuple tuple;

        while ( (tuple = inputOperator.getNextTuple()) != null) {
            list.add(tuple);
        }
        result = list.stream()
            .map(e -> new Tuple(outputSchema, e.getField(predicate.getNameColumn()), e.getField(predicate.getDataColumn())))
            .collect(Collectors.toList());
    }


}
