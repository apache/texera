package edu.uci.ics.texera.dataflow.twitter;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * Provides necessary properties needed by {@link TwitterConverter} operator
 * 
 * twitterJsonStringFieldName is the field name of the raw twitter JSON data. 
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterConverterPredicate extends PredicateBase {
    
    private final String twitterJsonStringFieldName;
        
    @JsonCreator
    public TwitterConverterPredicate(
            @JsonProperty(value = PropertyNameConstants.TWITTER_CONVERTER_RAW_JSON, required = true)
            String twitterJsonStringFieldName
            ) {
        // the field name must not be null or empty
        Preconditions.checkNotNull(twitterJsonStringFieldName);
        Preconditions.checkArgument(!twitterJsonStringFieldName.isEmpty(), "twitter raw json string field name cannot be empty");
        
        this.twitterJsonStringFieldName = twitterJsonStringFieldName;
    }
    
    @JsonProperty(value = PropertyNameConstants.TWITTER_CONVERTER_RAW_JSON)
    public String getTwitterJsonStringFieldName() {
        return this.twitterJsonStringFieldName;
    }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Convert Twitter")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "onvert the raw twitter data to readable records")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }

}