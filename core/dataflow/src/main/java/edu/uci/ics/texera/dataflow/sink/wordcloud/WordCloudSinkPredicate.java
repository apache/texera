package edu.uci.ics.texera.dataflow.sink.wordcloud;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;


public class WordCloudSinkPredicate extends PredicateBase {

    private String wordColumn;
    private String countColumn;


    @JsonCreator
    public WordCloudSinkPredicate(@JsonProperty(required = true, value = PropertyNameConstants.WORD_COLUMN)
                                  String wordColumn,
                                  @JsonProperty(required = true, value = PropertyNameConstants.COUNT_COLUMN)
                                  String countColumn
    ) {
        this.wordColumn = wordColumn;
        this.countColumn = countColumn;
    }

    @JsonProperty(value = PropertyNameConstants.WORD_COLUMN)
    public String getWordColumn() {
        return wordColumn;
    }

    @JsonProperty(value = PropertyNameConstants.COUNT_COLUMN)
    public String getCountColumn() {
        return countColumn;
    }

    @Override
    public WordCloudSink newOperator() {
        return new WordCloudSink(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Word cloud (used with word count)")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the word count in visual approach")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
            .build();
    }
}
