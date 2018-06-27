/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.schema.Schema.Builder;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;

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

    public Aggregator(AggregatorPredicate predicate)
    {
        this.predicate = predicate;
    }
    
    private boolean isAggregationTypeAllowed(AttributeType inputAttrType, AggregationType aggType)
    {
        boolean retVal;
        switch(inputAttrType)
        {
        case _ID_TYPE:
        case LIST:
            if(Arrays.asList(AggregationType.COUNT).contains(aggType))
            {
                retVal = true;
            }
            else
            {
                retVal = false;
            }
            break;
        case DATE:
        case STRING:
        case TEXT:
            if(Arrays.asList(AggregationType.COUNT, AggregationType.MIN, AggregationType.MAX).contains(aggType))
            {
                retVal = true;
            }
            else
            {
                retVal = false;
            }
            break;
        case INTEGER:
        case DOUBLE:
            if(Arrays.asList(AggregationType.COUNT, AggregationType.MIN, AggregationType.MAX, AggregationType.AVERAGE, AggregationType.SUM).contains(aggType))
            {
                retVal = true;
            }
            else
            {
                retVal = false;
            }
            break;
        default:
            retVal = false;
        }
        
        return retVal;
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
            if(!isAggregationTypeAllowed(inputSchema.getAttribute(aggregationItems.get(i).getAttributeName()).getType(), aggregationItems.get(i).getAggregatorType()))
            {
                throw new TexeraException(
                        AggregatorErrorMessages.ATTRIBUTE_TYPE_NOT_FIT_FOR_AGGREGATION(aggregationItems.get(i).getAttributeName(),
                                aggregationItems.get(i).getAggregatorType().toString()));
            }
        }

        outputSchema = transformToOutputSchema(inputSchema);
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        Builder schemaBuilder = new Schema.Builder();
        for(int i=0; i<aggregationItems.size(); i++)
        {
            schemaBuilder = schemaBuilder.add(aggregationItems.get(i).getResultAttributeName(), inputSchema[0].getAttribute(aggregationItems.get(i).getAttributeName()).getType());
        }

        return schemaBuilder.build();
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
        Tuple inputTuple = inputOperator.getNextTuple();
        List<IField> aggregatedResults = new ArrayList<IField>();
        if(inputTuple != null)
        {
            aggregatedResults = initialiseResultFieldsList(inputTuple);
            aggregatedResults.set(0, new IntegerField((int)aggregatedResults.get(0).getValue() + 1));
        }
        
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
                        if(compare((Integer)field.getValue(), (Integer)aggregatedResults.get(i+1).getValue()) < 0)
                        {
                            aggregatedResults.set(i+1, new IntegerField((int)field.getValue()));
                        }
                        break;
                    case DOUBLE:
                        if(compare((Double)field.getValue(), (Double)aggregatedResults.get(i+1).getValue()) < 0)
                        {
                            aggregatedResults.set(i+1, new DoubleField((double)field.getValue()));
                        }
                        break;
                    case TEXT:
                        if(compare((String)field.getValue(), (String)aggregatedResults.get(i+1).getValue()) < 0)
                        {
                            aggregatedResults.set(i+1, new TextField((String)field.getValue()));
                        }
                        break;
                    case STRING:
                        if(compare((String)field.getValue(), (String)aggregatedResults.get(i+1).getValue()) < 0)
                        {
                            aggregatedResults.set(i+1, new StringField((String)field.getValue()));
                        }
                        break;
                    case DATE:
                        if(compare((LocalDate)field.getValue(), (LocalDate)aggregatedResults.get(i+1).getValue()) < 0)
                        {
                            aggregatedResults.set(i+1, new DateField((LocalDate)field.getValue()));
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
                        if(compare((Integer)field.getValue(), (Integer)aggregatedResults.get(i+1).getValue()) > 0)
                        {
                            aggregatedResults.set(i+1, new IntegerField((int)field.getValue()));
                        }
                        break;
                    case DOUBLE:
                        if(compare((Double)field.getValue(), (Double)aggregatedResults.get(i+1).getValue()) > 0)
                        {
                            aggregatedResults.set(i+1, new DoubleField((double)field.getValue()));
                        }
                        break;
                    case TEXT:
                        if(compare((String)field.getValue(), (String)aggregatedResults.get(i+1).getValue()) > 0)
                        {
                            aggregatedResults.set(i+1, new TextField((String)field.getValue()));
                        }
                        break;
                    case STRING:
                        if(compare((String)field.getValue(), (String)aggregatedResults.get(i+1).getValue()) > 0)
                        {
                            aggregatedResults.set(i+1, new StringField((String)field.getValue()));
                        }
                        break;
                    case DATE:
                        if(compare((LocalDate)field.getValue(), (LocalDate)aggregatedResults.get(i+1).getValue()) > 0)
                        {
                            aggregatedResults.set(i+1, new DateField((LocalDate)field.getValue()));
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
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.INTEGER, aggregatedResults.get(i+1));
                    break;
                case DOUBLE:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, aggregatedResults.get(i+1));
                    break;
                case TEXT:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.TEXT, aggregatedResults.get(i+1));
                    break;
                case STRING:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.STRING, aggregatedResults.get(i+1));
                    break;
                case DATE:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DATE, aggregatedResults.get(i+1));
                    break;
                }
                break;
            }
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

    private <T extends Comparable<T>> int compare(T a, T b){
        return a.compareTo(b);
    }
    
    private List<IField> initialiseResultFieldsList(Tuple firstTuple)
    {
        List<IField> aggregatedResults = new ArrayList<IField>();
        
        //add count variable initially. This will be there no matter what the aggregations.
        aggregatedResults.add(new IntegerField(0));
        
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        
        List<Attribute> inputSchemaAttributes = inputSchema.getAttributes();
        for(int i=0; i< aggregationItems.size(); i++)
        {
            if(aggregationItems.get(i).getAggregatorType() == AggregationType.COUNT)
            {
                aggregatedResults.add(new IntegerField(1));
                continue;
            }
            Attribute inputAttr= inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
            IField fieldEntry = firstTuple.getField(aggregationItems.get(i).getAttributeName());
            switch(inputAttr.getType())
            {
            case INTEGER:
                aggregatedResults.add(new IntegerField((Integer)fieldEntry.getValue()));
                break;
            case DOUBLE:
                aggregatedResults.add(new DoubleField((Double)fieldEntry.getValue()));
                break;
            case DATE:
                aggregatedResults.add(new DateField((LocalDate)fieldEntry.getValue()));
                break;
            case TEXT:
                aggregatedResults.add(new TextField((String)fieldEntry.getValue()));
                break;
            case STRING:
                aggregatedResults.add(new StringField((String)fieldEntry.getValue()));
                break;
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
