package edu.uci.ics.texera.dataflow.sink.wordcloud;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;
import java.util.ArrayList;
import java.util.List;


public class WordCloudSink extends VisualizationOperator {
    private final int MAX_FONT_SIZE = 200;
    private final int MIN_FONT_SIZE = 50;
    private WordCloudSinkPredicate predicate;
    private String WORD = "word";
    private String COUNT = "count";
    public WordCloudSink(WordCloudSinkPredicate predicate) {
        super(VisualizationConstants.WORD_CLOUD);
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

        Attribute wordColumn =  schema.getAttribute(this.WORD);
        AttributeType nameColumnType = wordColumn.getType();
        if (!nameColumnType.equals(AttributeType.STRING) && !nameColumnType.equals(AttributeType.TEXT)) {
            throw new DataflowException("Type of name column should be string or text.");
        }

        Attribute countColumn =  schema.getAttribute(this.COUNT);
        AttributeType dataColumnType = countColumn.getType();
        if (!dataColumnType.equals(AttributeType.DOUBLE) && !dataColumnType.equals(AttributeType.INTEGER)) {
            throw new DataflowException(("Type of data column should be integer or double."));
        }

        outputSchema = new Schema.Builder().add(wordColumn, countColumn).build();
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        List<Tuple> list = new ArrayList<>();

        Tuple tuple;

        while ( (tuple = inputOperator.getNextTuple()) != null) {
            list.add(tuple);
        }

        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;
        List<Tuple> tempList = new ArrayList<>();
        for (Tuple t: list) {
            double value = VisualizationOperator.extractNumber(t.getField(this.COUNT));
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);

        }
        // normalize the font size for wordcloud js
        // https://github.com/timdream/wordcloud2.js/issues/53
        for (Tuple t: list) {
            double value = VisualizationOperator.extractNumber(t.getField(this.COUNT));
            tempList.add(new Tuple(outputSchema, t.getField(this.WORD), new IntegerField(
                (int) ((value - minValue) / (maxValue - minValue) * (this.MAX_FONT_SIZE - this.MIN_FONT_SIZE) + this.MIN_FONT_SIZE)) ));
        }

        this.result = tempList;

    }


}
