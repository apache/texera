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
    
    private final List<AggregationAttributeAndResult> attributeAggregateResultList;

    @JsonCreator
    public AggregatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_AGGREGATOR_RESULT_LIST, required = true)
            List<AggregationAttributeAndResult> attributeAggResultList
            )
    {
        this.attributeAggregateResultList = attributeAggResultList;
    }

    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_AGGREGATOR_RESULT_LIST)
    public List<AggregationAttributeAndResult> getAttributeAggregateResultList()
    {
        return attributeAggregateResultList;
    }

    @Override
    public IOperator newOperator() {
        return new Aggregator(this);
    }
}
