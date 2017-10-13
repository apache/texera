/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.comparablematcher.ComparablePredicate;

/**
 * @author avinash
 * 
 * Aggregator operator is used to perform aggregation (like sum, count, min, max, average) operations on a column.
 *
 */
public class Aggregator extends AbstractSingleInputOperator
{
    private final AggregatorPredicate predicate;
    
    private Schema inputSchema;
    private Schema outputSchema;

    public Aggregator(AggregatorPredicate predicate)
    {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws TexeraException
    {
        inputSchema = inputOperator.getOutputSchema();
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());

        if(predicate.getAggregatorType() != AggregationType.COUNT)
        {
            if(Schema.isAttributeType(inputSchema, predicate.getAttributeName(), AttributeType._ID_TYPE)
               || Schema.isAttributeType(inputSchema, predicate.getAttributeName(), AttributeType.DATE)
               || Schema.isAttributeType(inputSchema, predicate.getAttributeName(), AttributeType.LIST)
               || Schema.isAttributeType(inputSchema, predicate.getAttributeName(), AttributeType.STRING)
               || Schema.isAttributeType(inputSchema, predicate.getAttributeName(), AttributeType.TEXT))
            {
                throw new TexeraException(
                        ErrorMessages.ATTRIBUTE_TYPE_NOT_FIT_FOR_AGGREGATION(predicate.getAttributeName(),  predicate.getAggregatorType().toString()));
            }
        }

        outputSchema = new Schema.Builder().add(predicate.getResultAttributeName(), AttributeType.DOUBLE).build();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException
    {
        if (cursor == CLOSED || cursor >= 1) {
            return null;
        }
        Tuple resultTuple = null;
        resultTuple = processAllTuples(inputOperator);
        cursor++;
        return resultTuple;
    }

    public Tuple processAllTuples(IOperator inputOperator)
    {
        Tuple inputTuple;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        double count = 0;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null)
        {
            IField field = inputTuple.getField(predicate.getAttributeName());
            switch(predicate.getAggregatorType())
            {
            case MIN:
                if((double)field.getValue() < min)
                {
                    min = (double)field.getValue();
                }
                break;
            case MAX:
                if((double)field.getValue() > max)
                {
                    max = (double)field.getValue();
                }
                break;
            case AVERAGE:
                sum += (double)field.getValue();
                count++;
                break;
            case COUNT:
                count++;
                break;
            }
        }
        
        switch(predicate.getAggregatorType())
        {
        case MIN:
            return new Tuple.Builder().add(predicate.getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(min)).build();
        case MAX:
            return new Tuple.Builder().add(predicate.getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(max)).build();
        case AVERAGE:
            return new Tuple.Builder().add(predicate.getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(sum/count)).build();
        case COUNT:
            return new Tuple.Builder().add(predicate.getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(count)).build();
        default:
            return null;
        }
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException
    {
        return null;
    }

    @Override
    protected void cleanUp() throws TexeraException
    {
    }

    public AggregatorPredicate getPredicate()
    {
        return this.predicate;
    }
}
