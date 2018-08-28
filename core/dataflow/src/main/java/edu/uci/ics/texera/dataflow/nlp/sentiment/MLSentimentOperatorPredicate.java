package edu.uci.ics.texera.dataflow.nlp.sentiment;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class MLSentimentOperatorPredicate extends PredicateBase {

    private final String inputAttributeName;
    private final String resultAttributeName;
    private final int batchSize;

    @JsonCreator
    public MLSentimentOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
                    String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
                    String resultAttributeName,

            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.NLP_NLTK_BATCH_SIZE, required = true,
                    defaultValue = "10")
                    int batchSize) {
        if (inputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Input Attribute Name Cannot Be Empty");
        }
        if (resultAttributeName.trim().isEmpty()) {
            throw new TexeraException("Result Attribute Name Cannot Be Empty");
        }
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.batchSize = batchSize;
    };

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }

    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }

    @JsonProperty(PropertyNameConstants.NLP_NLTK_BATCH_SIZE)
    public int getBatchSize() {
        return this.batchSize;
    }

    @Override
    public MLSentimentOperator newOperator() {
        return new MLSentimentOperator(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
                .put(PropertyNameConstants.USER_FRIENDLY_NAME, "ML Sentiment Analysis")
                .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Sentiment analysis based on machine learning package of UCLA")
                .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
                .build();
    }

}
