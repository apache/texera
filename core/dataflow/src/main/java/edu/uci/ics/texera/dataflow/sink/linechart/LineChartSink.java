package edu.uci.ics.texera.dataflow.sink.linechart;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LineChartSink is a sink that can be used by to get tuples for line chart.
 * It returns tuples with data name (String) and at least one number (Integer or Double).
 * @author Mingji Han
 *
 */
public class LineChartSink extends VisualizationOperator {

    LineChartSinkPredicate predicate;
    private List<Attribute> attributes = new ArrayList<>();
    public LineChartSink(LineChartSinkPredicate predicate) {
        super(predicate.getLineChartEnum().getChartStyle());
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

        attributes.add(nameColumn);

        List<String> dataColumns = predicate.getDataColumn();
        for (String name: dataColumns) {
            Attribute dataColumn =  schema.getAttribute(name);
            AttributeType dataColumnType = dataColumn.getType();
            if (!dataColumnType.equals(AttributeType.DOUBLE) && !dataColumnType.equals(AttributeType.INTEGER)) {
                throw new DataflowException(("Type of data column should be integer or double."));
            }
            attributes.add(dataColumn);
        }


        outputSchema = new Schema.Builder().add(attributes).build();
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
                .map(e -> { IField[] fields = attributes.stream()
                        .map(a -> e.getField(a.getName())).toArray(IField[]::new);
                    return new Tuple(outputSchema, fields);
                })
                .collect(Collectors.toList());
    }
}
