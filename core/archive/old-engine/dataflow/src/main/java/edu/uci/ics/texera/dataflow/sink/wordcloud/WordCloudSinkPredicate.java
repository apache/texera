package edu.uci.ics.texera.dataflow.sink.wordcloud;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;


public class WordCloudSinkPredicate extends PredicateBase {
    private String attribute;
    private String analyzer;

    @JsonCreator
    public WordCloudSinkPredicate(@JsonProperty(required = true, value = PropertyNameConstants.ATTRIBUTE_NAME)
                                    String attribute,
                                  @AdvancedOption
                                  @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true,
                                          defaultValue = LuceneAnalyzerConstants.STANDARD_ANALYZER)
                                          String analyzer
    ) {
        this.attribute = attribute;
        this.analyzer = analyzer;
    }


    @Override
    public WordCloudSink newOperator() {
        return new WordCloudSink(this);
    }

    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttribute() {
        return attribute;
    }

    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerString() {
        return this.analyzer;
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Word cloud")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the word count in visual approach")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
            .build();
    }
}
