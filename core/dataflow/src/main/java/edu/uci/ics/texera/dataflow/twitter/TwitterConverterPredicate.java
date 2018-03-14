package edu.uci.ics.texera.dataflow.twitter;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class TwitterConverterPredicate extends PredicateBase {
        
    @JsonCreator
    public TwitterConverterPredicate() { }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter();
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Convert Twitter")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "onvert the raw twitter data to readable records")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }

}