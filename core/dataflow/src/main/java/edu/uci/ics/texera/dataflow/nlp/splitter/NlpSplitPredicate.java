package edu.uci.ics.texera.dataflow.nlp.splitter;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class NlpSplitPredicate extends PredicateBase {
    
    private final NLPOutputType outputType;
    private final String inputAttributeName;
    private final String resultAttributeName;
    //make a variable "outputType" that takes two values, one for one to one transformation
    // and another for one to many transformation with one to many as default
    
    @JsonCreator
    public NlpSplitPredicate(
            @JsonProperty(value = PropertyNameConstants.NLP_OUTPUT_TYPE, required = true)
            NLPOutputType outputType,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
            ) {
        this.outputType = outputType;
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
    }
    
    
    @JsonProperty(PropertyNameConstants.NLP_OUTPUT_TYPE)
    public NLPOutputType getOutputType() {
        return this.outputType;
    }
    
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }

    @Override
    public NlpSplitOperator newOperator() {
        return new NlpSplitOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Nlp Sentence Split")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Automatically split the text into multiple sentences using Natural Language Processing")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SPLIT_GROUP)
            .build();
    }

}
