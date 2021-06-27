package edu.uci.ics.texera.dataflow.projection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class ProjectionPredicate extends PredicateBase {
    
    private final List<String> projectionFields;
    
    @JsonCreator
    public ProjectionPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> projectionFields) {
        this.projectionFields = projectionFields.stream().map(s -> s.toLowerCase()).collect(Collectors.toList());
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getProjectionFields() {
        return new ArrayList<>(projectionFields);
    }
    
    @Override
    public ProjectionOperator newOperator() {
        return new ProjectionOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Projection")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Select a subset of columns")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }
    
}
