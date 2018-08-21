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
import edu.uci.ics.texera.api.field.*;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.schema.Schema.Builder;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;

/**
 * Aggregator operator is used to perform aggregation (like sum, count, min, max, average) operations on a column. The attribute types supported for
 * aggregation are DateField, DateTimeField, IntegerField, DoubleField, TextField, StringField, but not all fields support all types of aggregation.
 * DateField, DateTimeField, TextField, StringField only support MIN and MAX. However, COUNT is supported by all attribute types. Users can also do multiple
 * aggregations at a time i.e. MIN on a attribute A and SUM on attribute B. This is supported as AggregatorPredicate accepts a list of aggregations as
 * input.
 *
 * @author avinash
 */
public class Aggregator extends AbstractSingleInputOperator {
    private final AggregatorPredicate predicate;

    private Schema inputSchema;

    private int rowsCount = 0;

    public Aggregator(AggregatorPredicate predicate) {
        this.predicate = predicate;
    }

    private int getRowsCount() {
        return rowsCount;
    }

    private void incrementRowsProcessed() {
        rowsCount++;
    }

    /***
     * Not all aggregations are allowed for different attribute types. eg: Average makes no sense for Text. This function checks if the aggregation and attribute type
     * selected are compatible.
     * @param inputAttrType
     * @param aggType
     * @return
     */
    private boolean isAggregationTypeAllowed(AttributeType inputAttrType, AggregationType aggType) {
        boolean retVal = false;
        switch (aggType) {
            case COUNT:
                retVal = true;
                break;

            case MAX:
            case MIN:
                if (Arrays.asList(AttributeType.DATETIME, AttributeType.DATE, AttributeType.STRING, AttributeType.TEXT, AttributeType.INTEGER, AttributeType.DOUBLE).contains(inputAttrType)) {
                    retVal = true;
                }
                break;

            case SUM:
            case AVERAGE:
                if (Arrays.asList(AttributeType.INTEGER, AttributeType.DOUBLE).contains(inputAttrType)) {
                    retVal = true;
                }
                break;
        }

        return retVal;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        for (AggregationAttributeAndResult aggregationItem : aggregationItems) {
            Schema.checkAttributeExists(inputSchema, aggregationItem.getAttributeName());
            Schema.checkAttributeNotExists(inputSchema, aggregationItem.getResultAttributeName());
        }

        for (AggregationAttributeAndResult aggregationItem : aggregationItems) {
            if (!isAggregationTypeAllowed(inputSchema.getAttribute(aggregationItem.getAttributeName()).getType(), aggregationItem.getAggregatorType())) {
                throw new TexeraException(
                        AggregatorErrorMessages.ATTRIBUTE_TYPE_NOT_FIT_FOR_AGGREGATION(aggregationItem.getAttributeName(),
                                aggregationItem.getAggregatorType().toString()));
            }
        }

        outputSchema = transformToOutputSchema(inputSchema);
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        Builder schemaBuilder = new Schema.Builder();
        for (AggregationAttributeAndResult aggregationItem : aggregationItems) {
            schemaBuilder = schemaBuilder.add(aggregationItem.getResultAttributeName(), inputSchema[0].getAttribute(aggregationItem.getAttributeName()).getType());
        }

        return schemaBuilder.build();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        if (cursor == CLOSED || cursor >= 1) {
            return null;
        }
        Tuple resultTuple = null;
        List<IField> aggregatedResults = processAllTuples(inputOperator);
        resultTuple = putResultsIntoTuple(aggregatedResults);
        cursor++;
        return resultTuple;
    }

    /**
     * Processes all the tuples from input operator and generates the aggregations requested by the user. The aggregations
     * are put in a list. As the tuples from input operator are processed, the aggregations are updated in the list.
     * @param inputOperator
     * @return List<IField>
     */
    private List<IField> processAllTuples(IOperator inputOperator) {
        Tuple inputTuple = inputOperator.getNextTuple();
        List<IField> aggregatedResults = new ArrayList<IField>();
        if (inputTuple != null) {
            aggregatedResults = initializeResultFieldsList(inputTuple);
            incrementRowsProcessed();
        }

        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            incrementRowsProcessed();

            for (int i = 0; i < aggregationItems.size(); i++) {
                IField field = inputTuple.getField(aggregationItems.get(i).getAttributeName());
                Attribute inputAttr = inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());

                switch (aggregationItems.get(i).getAggregatorType()) {
                    case MIN: {
                        if (compare(field, aggregatedResults.get(i), inputAttr.getType()) < 0) {
                            aggregatedResults.set(i, field);
                        }
                        break;
                    }
                    case MAX: {
                        if (compare(field, aggregatedResults.get(i), inputAttr.getType()) > 0) {
                            aggregatedResults.set(i, field);
                        }
                        break;
                    }
                    case AVERAGE:
                    case SUM: {
                        //We calculate sum and then at last divide it by totalRowsCount
                        switch (inputAttr.getType()) {
                            case INTEGER:
                                aggregatedResults.set(i, new IntegerField((int) aggregatedResults.get(i).getValue() + (int) field.getValue()));
                                break;
                            case DOUBLE:
                                aggregatedResults.set(i, new DoubleField((double) aggregatedResults.get(i).getValue() + (double) field.getValue()));
                                break;
                        }
                        break;
                    }
                    case COUNT:
                        aggregatedResults.set(i, new IntegerField((int) aggregatedResults.get(i).getValue() + 1));
                        break;
                }
            }
        }

        return aggregatedResults;
    }

    /**
     * Takes the aggregates generated after processing all the tuples and puts it into a Tuple which is sent to next operator.
     * @param aggregatedResults
     * @return Tuple
     */
    private Tuple putResultsIntoTuple(List<IField> aggregatedResults) {
        Tuple.Builder tupleBuilder = new Tuple.Builder();

        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();
        for (int i = 0; i < aggregationItems.size(); i++) {
            Attribute inputAttr = inputSchema.getAttribute(aggregationItems.get(i).getAttributeName());
            switch (aggregationItems.get(i).getAggregatorType()) {
                case MIN:
                case MAX:
                case SUM: {
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), inputAttr.getType(), aggregatedResults.get(i));
                    break;
                }

                case AVERAGE: {
                    switch (inputAttr.getType()) {
                        case INTEGER:
                            tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField(((int) aggregatedResults.get(i).getValue()) * 1.0 / getRowsCount() * 1.0));
                            break;
                        case DOUBLE:
                            tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.DOUBLE, new DoubleField((double) aggregatedResults.get(i).getValue() / getRowsCount() * 1.0));
                            break;
                    }
                    break;
                }
                case COUNT: {
                    tupleBuilder.add(aggregationItems.get(i).getResultAttributeName(), AttributeType.INTEGER, aggregatedResults.get(i));
                    break;
                }
            }
        }

        return tupleBuilder.build();
    }

    private int compare(IField a, IField b, AttributeType attrType) {
        int retVal = 0;
        switch (attrType) {
            case INTEGER:
                retVal = ((IntegerField) a).getValue().compareTo(((IntegerField) b).getValue());
                break;
            case DOUBLE:
                retVal = ((DoubleField) a).getValue().compareTo(((DoubleField) b).getValue());
                break;
            case TEXT:
                retVal = ((TextField) a).getValue().compareTo(((TextField) b).getValue());
                break;
            case STRING:
                retVal = ((StringField) a).getValue().compareTo(((StringField) b).getValue());
                break;
            case DATE:
                retVal = ((DateField) a).getValue().compareTo(((DateField) b).getValue());
                break;
            case DATETIME:
                retVal = ((DateTimeField) a).getValue().compareTo(((DateTimeField) b).getValue());
                break;
        }

        return retVal;
    }

    private List<IField> initializeResultFieldsList(Tuple firstTuple) {
        List<IField> aggregatedResults = new ArrayList<IField>();

        List<AggregationAttributeAndResult> aggregationItems = predicate.getAttributeAggregateResultList();

        for (AggregationAttributeAndResult aggregationItem : aggregationItems) {
            if (aggregationItem.getAggregatorType() == AggregationType.COUNT) {
                aggregatedResults.add(new IntegerField(1));
            } else {
                IField fieldEntry = firstTuple.getField(aggregationItem.getAttributeName());
                aggregatedResults.add(fieldEntry);
            }
        }

        return aggregatedResults;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        return null;
    }

    @Override
    protected void cleanUp() throws TexeraException {
    }

    public AggregatorPredicate getPredicate() {
        return this.predicate;
    }
}
