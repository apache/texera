/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.schema.Schema.Builder;
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
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        for(int i=0; i<aggregationItems.size(); i++)
        {
            Schema.checkAttributeExists(inputSchema, aggregationItems.get(i).getAttributeName());
            Schema.checkAttributeNotExists(inputSchema, aggregationItems.get(i).getResultAttributeName());
        }

        for(int i=0; i<aggregationItems.size(); i++)
        {
            if(aggregationItems.get(i).getAggregatorType() != AggregationType.COUNT)
            {
                if(Schema.isAttributeType(inputSchema, aggregationItems.get(i).getAttributeName(), AttributeType._ID_TYPE)
                   || Schema.isAttributeType(inputSchema, aggregationItems.get(i).getAttributeName(), AttributeType.DATE)
                   || Schema.isAttributeType(inputSchema, aggregationItems.get(i).getAttributeName(), AttributeType.LIST)
                   || Schema.isAttributeType(inputSchema, aggregationItems.get(i).getAttributeName(), AttributeType.STRING)
                   || Schema.isAttributeType(inputSchema, aggregationItems.get(i).getAttributeName(), AttributeType.TEXT))
                {
                    throw new TexeraException(
                            ErrorMessages.ATTRIBUTE_TYPE_NOT_FIT_FOR_AGGREGATION(aggregationItems.get(i).getAttributeName(),  aggregationItems.get(i).getAggregatorType().toString()));
                }
            }
        }

        Builder schemaBuilder = new Schema.Builder();
        for(int i=0; i<aggregationItems.size(); i++)
        {
            schemaBuilder = schemaBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE);
        }
        
        outputSchema = schemaBuilder.build();
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
        List<IField> aggregatedResults = initialiseResultFieldsList();
        
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        
        while ((inputTuple = inputOperator.getNextTuple()) != null)
        {
            //incrementing the count
            aggregatedResults.set(0, new IntegerField((int)aggregatedResults.get(0).getValue() + 1));
            
            for(int i=0; i<aggregationItems.size(); i++)
            {
                IField field = inputTuple.getField(aggregationItems.get(i).getAttributeName());
                Attribute inputAttr= inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
                
                switch(aggregationItems.get(i).getAggregatorType())
                {
                case MIN:
                {
                    switch(inputAttr.getType())
                    {
                    case INTEGER:
                        if((int)field.getValue() < (int)aggregatedResults.get(i+1).getValue())
                        {
                            aggregatedResults.set(i+1, new IntegerField((int)field.getValue()));
                        }
                        break;
                    case DOUBLE:
                        if((double)field.getValue() < (double)aggregatedResults.get(i+1).getValue())
                        {
                            aggregatedResults.set(i+1, new DoubleField((double)field.getValue()));
                        }
                        break;
                    }
                    break;
                }
                case MAX:
                {
                    switch(inputAttr.getType())
                    {
                    case INTEGER:
                        if((int)field.getValue() > (int)aggregatedResults.get(i+1).getValue())
                        {
                            aggregatedResults.set(i+1, new IntegerField((int)field.getValue()));
                        }
                        break;
                    case DOUBLE:
                        if((double)field.getValue() > (double)aggregatedResults.get(i+1).getValue())
                        {
                            aggregatedResults.set(i+1, new DoubleField((double)field.getValue()));
                        }
                        break;
                    }
                    break;
                }
                case AVERAGE:
                case SUM:
                {
                  //We calculate sum and then at last divide it by aggregatedResults.get(0)
                    switch(inputAttr.getType())
                    {
                    case INTEGER:
                        aggregatedResults.set(i+1, new IntegerField((int)aggregatedResults.get(i+1).getValue() + (int)field.getValue()));
                        break;
                    case DOUBLE:
                        aggregatedResults.set(i+1, new DoubleField((double)aggregatedResults.get(i+1).getValue() + (double)field.getValue()));
                        break;
                    }
                    break;
                }
                case COUNT:
                    aggregatedResults.set(i+1, new IntegerField((int)aggregatedResults.get(i+1).getValue() + 1));
                    break;
                }
            }
        }
        
        Tuple.Builder tupleBuilder = new Tuple.Builder();
        
        for(int i=0; i<aggregationItems.size(); i++)
        {
            Attribute inputAttr= inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
            switch(aggregationItems.get(i).getAggregatorType())
            {
            case MIN:
            case MAX:
            case SUM:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.INTEGER, aggregatedResults.get(i+1));
                    break;
                case DOUBLE:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, aggregatedResults.get(i+1));
                    break;
                }
                break;
            }
            case AVERAGE:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(((int)aggregatedResults.get(i+1).getValue())*1.0/((int)aggregatedResults.get(0).getValue())*1.0));
                    break;
                case DOUBLE:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField((double)aggregatedResults.get(i+1).getValue()/((int)aggregatedResults.get(0).getValue())*1.0));
                    break;
                }
                break;
            }
            case COUNT:
            {
                tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.INTEGER, aggregatedResults.get(i+1));
                break;
            }
            }
        }
        
        return tupleBuilder.build();
    }
    
    private List<IField> initialiseResultFieldsList()
    {
        List<IField> aggregatedResults = new ArrayList<IField>();
        
        //add count variable initially. This will be there no matter what the aggregations.
        aggregatedResults.add(new IntegerField(0));
        
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        
        List<Attribute> inputSchemaAttributes = inputSchema.getAttributes();
        for(int i=0; i< aggregationItems.size(); i++)
        {
            Attribute inputAttr= inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
            switch(aggregationItems.get(i).getAggregatorType())
            {
            case MIN:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    aggregatedResults.add(new IntegerField(Integer.MAX_VALUE));
                    break;
                case DOUBLE:
                    aggregatedResults.add(new DoubleField(Double.MAX_VALUE));
                    break;
                }
                break;
            }
            case MAX:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    aggregatedResults.add(new IntegerField(Integer.MIN_VALUE));
                    break;
                case DOUBLE:
                    aggregatedResults.add(new DoubleField(Double.MIN_VALUE));
                    break;
                }
                break;
            }
            case AVERAGE:
            case SUM:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    aggregatedResults.add(new IntegerField(0));
                    break;
                case DOUBLE:
                    aggregatedResults.add(new DoubleField(0.0));
                    break;
                }
                break;
            }
            case COUNT:
            {
                aggregatedResults.add(new IntegerField(0));
                break;
            }
            }
        }
        
        return aggregatedResults;
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
