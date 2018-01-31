package edu.uci.ics.texera.dataflow.nlp.sentiment;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class NlpSentimentPredicate extends PredicateBase {
    
    private final String inputAttributeName;
    private final String resultAttributeName;
    
    public NlpSentimentPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
            ) {
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
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
    public IOperator newOperator() {
        return new NlpSentimentOperator(this);
    }

}
