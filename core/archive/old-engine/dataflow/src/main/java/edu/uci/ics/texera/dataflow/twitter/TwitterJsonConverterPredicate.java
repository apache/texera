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
 * Provides necessary properties needed by {@link TwitterJsonConverter} operator
 * 
 * twitterJsonStringAttributeName is the field name of the raw twitter JSON data.
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterJsonConverterPredicate extends PredicateBase {
    
    private final String twitterJsonStringAttributeName;
        
    @JsonCreator
    public TwitterJsonConverterPredicate(
            @JsonProperty(value = PropertyNameConstants.TWITTER_CONVERTER_RAW_JSON, required = true)
            String twitterJsonStringAttributeName
            ) {
        // the field name must not be null or empty
        Preconditions.checkNotNull(twitterJsonStringAttributeName);
        Preconditions.checkArgument(!twitterJsonStringAttributeName.isEmpty(), "twitter raw json string field name cannot be empty");
        
        this.twitterJsonStringAttributeName = twitterJsonStringAttributeName;
    }
    
    @JsonProperty(value = PropertyNameConstants.TWITTER_CONVERTER_RAW_JSON)
    public String getTwitterJsonStringAttributeName() {
        return this.twitterJsonStringAttributeName;
    }

    @Override
    public TwitterJsonConverter newOperator() {
        return new TwitterJsonConverter(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Convert Twitter JSON")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Convert the raw twitter JSON data into different fields of the tweet")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }

}