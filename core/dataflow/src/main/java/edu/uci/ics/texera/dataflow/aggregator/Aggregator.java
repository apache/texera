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

    private int rowsCount = 0;

    public Aggregator(AggregatorPredicate predicate)
    {
        this.predicate = predicate;
    }

    private int getRowsCount()
    {
        return rowsCount;
    }

    private void incrementRowsProcessed()
    {
        rowsCount++;
    }
    
    private boolean isAggregationTypeAllowed(AttributeType inputAttrType, AggregationType aggType)
    {
        boolean retVal = false;
        switch (aggType)
        {
            case COUNT:
                retVal = true;
                break;

            case MAX:
            case MIN:
                if(Arrays.asList(AttributeType.DATE, AttributeType.STRING, AttributeType.TEXT, AttributeType.INTEGER, AttributeType.DOUBLE).contains(inputAttrType))
                {
                    retVal = true;
                }
                break;

            case SUM:
            case AVERAGE:
                if(Arrays.asList(AttributeType.INTEGER, AttributeType.DOUBLE).contains(inputAttrType))
                {
                    retVal = true;
                }
                break;
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
            incrementRowsProcessed();
        }
        
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        
        while ((inputTuple = inputOperator.getNextTuple()) != null)
        {
            incrementRowsProcessed();
            
            for(int i=0; i<aggregationItems.size(); i++)
            {
                IField field = inputTuple.getField(aggregationItems.get(i).getAttributeName());
                Attribute inputAttr= inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
                
                switch(aggregationItems.get(i).getAggregatorType())
                {
                case MIN:
                {
                    if(compare(field, aggregatedResults.get(i), inputAttr.getType()) < 0)
                    {
                        aggregatedResults.set(i, field);
                    }
                    break;
                }
                case MAX:
                {
                    if(compare(field, aggregatedResults.get(i), inputAttr.getType()) > 0)
                    {
                        aggregatedResults.set(i, field);
                    }
                    break;
                }
                case AVERAGE:
                case SUM:
                {
                  //We calculate sum and then at last divide it by totalRowsCount
                    switch(inputAttr.getType())
                    {
                    case INTEGER:
                        aggregatedResults.set(i, new IntegerField((int)aggregatedResults.get(i).getValue() + (int)field.getValue()));
                        break;
                    case DOUBLE:
                        aggregatedResults.set(i, new DoubleField((double)aggregatedResults.get(i).getValue() + (double)field.getValue()));
                        break;
                    }
                    break;
                }
                case COUNT:
                    aggregatedResults.set(i, new IntegerField((int)aggregatedResults.get(i).getValue() + 1));
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
                tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), inputAttr.getType(), aggregatedResults.get(i));
                break;
            }
            case SUM:
            {
                tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), inputAttr.getType(), aggregatedResults.get(i));
                break;
            }
            case AVERAGE:
            {
                switch(inputAttr.getType())
                {
                case INTEGER:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(((int)aggregatedResults.get(i).getValue())*1.0/getRowsCount()*1.0));
                    break;
                case DOUBLE:
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField((double)aggregatedResults.get(i).getValue()/getRowsCount()*1.0));
                    break;
                }
                break;
            }
            case COUNT:
            {
                tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.INTEGER, aggregatedResults.get(i));
                break;
            }
            }
        }
        
        return tupleBuilder.build();
    }

    private int compare(IField a, IField b, AttributeType attrType){
        int retVal = 0;
        switch (attrType){
            case INTEGER:
                retVal = ((IntegerField)a).getValue().compareTo(((IntegerField)b).getValue());
                break;
            case DOUBLE:
                retVal = ((DoubleField)a).getValue().compareTo(((DoubleField)b).getValue());
                break;
            case TEXT:
                retVal =  ((TextField)a).getValue().compareTo(((TextField)b).getValue());
                break;
            case STRING:
                retVal = ((StringField)a).getValue().compareTo(((StringField)b).getValue());
                break;
            case DATE:
                retVal =  ((DateField)a).getValue().compareTo(((DateField)b).getValue());
                break;
        }

        return retVal;
    }
    
    private List<IField> initialiseResultFieldsList(Tuple firstTuple)
    {
        List<IField> aggregatedResults = new ArrayList<IField>();
        
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();

        for(int i=0; i< aggregationItems.size(); i++)
        {
            if(aggregationItems.get(i).getAggregatorType() == AggregationType.COUNT)
            {
                aggregatedResults.add(new IntegerField(1));
                continue;
            }
            IField fieldEntry = firstTuple.getField(aggregationItems.get(i).getAttributeName());
            aggregatedResults.add(fieldEntry);
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
