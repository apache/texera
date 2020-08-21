package edu.uci.ics.texera.dataflow.sink.wordcloud;


import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;

import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.VisualizationOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import org.apache.lucene.analysis.core.StopAnalyzer;

/**
 * WordCloudSink is a sink that can be used by the caller to generate data for wordcloud.js in frontend.
 * WordCloudSink returns tuples with word (String) and its font size (Integer or Double) for frontend.
 * @author Mingji Han
 *
 */
public class WordCloudSink extends VisualizationOperator {
    private final int MAX_FONT_SIZE = 200;
    private final int MIN_FONT_SIZE = 50;
    private WordCloudSinkPredicate predicate;

    private boolean addPayload = false;
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

        this.addPayload = ! inputOperator.getOutputSchema().containsAttribute(SchemaConstants.PAYLOAD);

        Schema schema = inputOperator.getOutputSchema();

        Attribute textColumn =  schema.getAttribute(predicate.getAttribute());
        AttributeType nameColumnType = textColumn.getType();
        if (!nameColumnType.equals(AttributeType.TEXT)) {
            throw new DataflowException("Type of name column should be text.");
        }

        outputSchema = new Schema.Builder().add(new Attribute("word", AttributeType.STRING),
                                                new Attribute("count", AttributeType.INTEGER))
                                           .build();
        cursor = OPENED;
    }


    public List<Map.Entry<String, Integer>> wordCount() {
        Tuple tuple;
        HashMap<String, Integer> wordCountMap = new HashMap<>();
        while ( (tuple = inputOperator.getNextTuple()) != null) {
            if (addPayload) {
                tuple = new Tuple.Builder(tuple).add(SchemaConstants.PAYLOAD_ATTRIBUTE,new ListField<Span>(
                        DataflowUtils.generatePayloadFromTuple(tuple, predicate.getLuceneAnalyzerString()))).build();
            }

            ListField<Span> payloadField = tuple.getField("payload");
            List<Span> payloadSpanList = payloadField.getValue();

            for (Span span : payloadSpanList) {
                if (span.getAttributeName().equals(predicate.getAttribute())) {
                    String key = span.getValue().toLowerCase();
                    if (!StopAnalyzer.ENGLISH_STOP_WORDS_SET.contains(key))
                        wordCountMap.put(key, wordCountMap.get(key)==null ? 1 : wordCountMap.get(key) + 1);
                }
            }
        }

        return wordCountMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public void processTuples() throws TexeraException {

        // calculate word frequencies
        List<Map.Entry<String, Integer>> wordCountList = wordCount();

        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        for (Map.Entry<String, Integer> e: wordCountList) {
            int frequency = e.getValue();
            minValue = Math.min(minValue, frequency);
            maxValue = Math.max(maxValue, frequency);

        }
        // normalize the font size for wordcloud js
        // https://github.com/timdream/wordcloud2.js/issues/53
        List<Tuple> tempList = new ArrayList<>();
        for (Map.Entry<String, Integer> e: wordCountList) {
            int frequency = e.getValue();
            tempList.add(new Tuple(outputSchema, new StringField(e.getKey()), new IntegerField(
                (int) ((frequency - minValue) / (maxValue - minValue) * (this.MAX_FONT_SIZE - this.MIN_FONT_SIZE) + this.MIN_FONT_SIZE)) ));

        }

        this.result = tempList;

    }

}
