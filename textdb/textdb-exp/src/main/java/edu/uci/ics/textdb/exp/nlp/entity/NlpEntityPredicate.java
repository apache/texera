package edu.uci.ics.textdb.exp.nlp.entity;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NlpEntityPredicate extends PredicateBase {
    
    private NlpEntityType nlpEntityType;
    private List<String> attributeNames;

    @JsonCreator
    public NlpEntityPredicate(
            @JsonProperty(value = PropertyNameConstants.NLP_ENTITY_TYPE, required = true)
            NlpEntityType nlpEntityType, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames) {
        this.nlpEntityType = nlpEntityType;
        this.attributeNames = attributeNames;
    }

    @JsonProperty(PropertyNameConstants.NLP_ENTITY_TYPE)
    public NlpEntityType getNlpEntityType() {
        return nlpEntityType;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(attributeNames);
    }
    
    @Override
    public IOperator newOperator() {
        return new NlpEntityOperator(this);
    }
    
}
