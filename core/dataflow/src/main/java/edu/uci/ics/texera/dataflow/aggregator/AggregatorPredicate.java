/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.comparablematcher.ComparableMatcher;

/**
 * @author avinash
 *
 *AggregatorPredicate is the predicate for the Aggregator Operator.
 */
public class AggregatorPredicate extends PredicateBase{
    
    private final String attributeName;
    private final AggregationType aggregatorType;
    private final String resultAttributeName;

    @JsonCreator
    public AggregatorPredicate(
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
    public IOperator newOperator() {
        return new Aggregator(this);
    }
}
