package edu.uci.ics.texera.dataflow.comparablematcher;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 *
 * @author Adrian Seungjin Lee
 * @author Zuozhi Wang
 */
public class ComparablePredicate extends PredicateBase {

    private String attributeName;
    private String compareToValue;
    private ComparisonType matchingType;

    @JsonCreator
    public ComparablePredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attributeName,
            @JsonProperty(value = PropertyNameConstants.COMPARISON_TYPE, required = true)
            ComparisonType matchingType,
            @JsonProperty(value = PropertyNameConstants.COMPARE_TO_VALUE, required = true)
            String compareToValue) {
        if (attributeName == null || attributeName.trim().isEmpty()) {
            throw new TexeraException("attribute cannot be empty");
        }
        this.compareToValue = compareToValue;
        this.attributeName = attributeName;
        this.matchingType = matchingType;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty(value = PropertyNameConstants.COMPARISON_TYPE)
    public ComparisonType getComparisonType() {
        return matchingType;
    }

    @JsonProperty(value = PropertyNameConstants.COMPARE_TO_VALUE)
    public String getCompareToValue() {
        return compareToValue;
    }

    @Override
    public ComparableMatcher newOperator() {
        return new ComparableMatcher(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Comparison")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Select data based on a condition (>, <, =, ..)")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }

}
