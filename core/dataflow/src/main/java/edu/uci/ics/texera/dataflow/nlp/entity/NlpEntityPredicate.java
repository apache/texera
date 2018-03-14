package edu.uci.ics.texera.dataflow.nlp.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class NlpEntityPredicate extends PredicateBase {
    
    private final NlpEntityType nlpEntityType;
    private final List<String> attributeNames;
    private final String resultAttribute;

    @JsonCreator
    public NlpEntityPredicate(
            @JsonProperty(value = PropertyNameConstants.NLP_ENTITY_TYPE, required = true)
            NlpEntityType nlpEntityType, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttribute) {
        
        if (attributeNames.isEmpty()) {
            throw new TexeraException("attributes should not be empty");
        }
        
        this.nlpEntityType = nlpEntityType;
        this.attributeNames = attributeNames;
        if (resultAttribute == null || resultAttribute.trim().isEmpty()) {
            this.resultAttribute = this.getID();
        } else {
            this.resultAttribute = resultAttribute;
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
    
    @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttribute() {
        return this.resultAttribute;
    }
    
    @Override
    public NlpEntityOperator newOperator() {
        return new NlpEntityOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Entity Recognition")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Recognize entities in the text (person, location, date, ..)")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }
    
}
