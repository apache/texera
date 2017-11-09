package edu.uci.ics.texera.dataflow.aggregator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class AggregationAttributeAndResult
{
    private final String attributeName;
    private final AggregationType aggregatorType;
    private final String resultAttributeName;
    
    @JsonCreator
    public AggregationAttributeAndResult(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attributeName,
            @JsonProperty(value = PropertyNameConstants.AGGREGATOR_TYPE, required = true)
            AggregationType aggregatorType,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
            )
    {
        this.attributeName = attributeName;
        this.aggregatorType = aggregatorType;
        this.resultAttributeName = resultAttributeName;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName()
    {
        return attributeName;
    }

    @JsonProperty(value = PropertyNameConstants.AGGREGATOR_TYPE)
    public AggregationType getAggregatorType()
    {
        return aggregatorType;
    }
    
    @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName()
    {
        return resultAttributeName;
    }
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    
    @Override
    public boolean equals(Object that) {
        return EqualsBuilder.reflectionEquals(this, that);
    }
}
