package edu.uci.ics.textdb.exp.nlp.entity;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NlpEntityPredicate extends PredicateBase {
    
    private final NlpEntityType nlpEntityType;
    private final List<String> attributeNames;
    private final String spanListName;

    @JsonCreator
    public NlpEntityPredicate(
            @JsonProperty(value = PropertyNameConstants.NLP_ENTITY_TYPE, required = true)
            NlpEntityType nlpEntityType, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        this.nlpEntityType = nlpEntityType;
        this.attributeNames = attributeNames;
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = this.getID();
        } else {
            this.spanListName = spanListName;
        }
    }

    @JsonProperty(PropertyNameConstants.NLP_ENTITY_TYPE)
    public NlpEntityType getNlpEntityType() {
        return nlpEntityType;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(attributeNames);
    }
    
    @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME)
    public String getSpanListName() {
        return this.spanListName;
    }
    
    @Override
    public IOperator newOperator() {
        return new NlpEntityOperator(this);
    }
    
}
