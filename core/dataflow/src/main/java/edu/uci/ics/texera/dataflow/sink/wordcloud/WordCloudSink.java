package edu.uci.ics.texera.dataflow.sink.wordcloud;


import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;

/**
 * WordCloudSink is a sink that can be used by the caller to generate data for wordcloud.js in frontend.
 * WordCloudSink returns tuple with word (String) and its font size (Integer or Double) for frontend.
 * @author Mingji Han
 *
 */
public class WordCloudSink extends VisualizationOperator {
    private final int MAX_FONT_SIZE = 200;
    private final int MIN_FONT_SIZE = 50;
    private WordCloudSinkPredicate predicate;

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

        Attribute wordColumn =  schema.getAttribute(predicate.getWordColumn());
        AttributeType nameColumnType = wordColumn.getType();
        if (!nameColumnType.equals(AttributeType.STRING) && !nameColumnType.equals(AttributeType.TEXT)) {
            throw new DataflowException("Type of name column should be string or text.");
        }

        Attribute countColumn =  schema.getAttribute(predicate.getCountColumn());
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
            double value = VisualizationOperator.extractNumber(t.getField(predicate.getCountColumn()));
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);

        }
        // normalize the font size for wordcloud js
        // https://github.com/timdream/wordcloud2.js/issues/53
        for (Tuple t: list) {
            double value = VisualizationOperator.extractNumber(t.getField(predicate.getCountColumn()));
            tempList.add(new Tuple(outputSchema, t.getField(predicate.getWordColumn()), new IntegerField(
                (int) ((value - minValue) / (maxValue - minValue) * (this.MAX_FONT_SIZE - this.MIN_FONT_SIZE) + this.MIN_FONT_SIZE)) ));
        }

        this.result = tempList;

    }


}
