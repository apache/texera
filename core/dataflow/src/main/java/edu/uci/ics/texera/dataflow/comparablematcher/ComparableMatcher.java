package edu.uci.ics.texera.dataflow.comparablematcher;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.*;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.*;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;

/**
 * ComparableMatcher is matcher for comparison query on any field which deals
 * with Comparable.
 *
 * @author Adrian Seungjin Lee
 * @author Zuozhi Wang
 */
public class ComparableMatcher extends AbstractSingleInputOperator {
    
    private ComparablePredicate predicate;
    private AttributeType inputAttrType;

    public ComparableMatcher(ComparablePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataflowException {
        outputSchema = inputOperator.getOutputSchema();
        if (! outputSchema.containsAttribute(predicate.getAttributeName())) {
            throw new DataflowException(String.format("attribute %s not contained in input schema %s",
                    predicate.getAttributeName(), outputSchema.getAttributeNames()));
        }
        inputAttrType = outputSchema.getAttribute(predicate.getAttributeName()).getType();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple;
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            Tuple resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                return resultTuple;
            }
        }
        return null;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        boolean conditionSatisfied = false;
        switch (this.inputAttrType) {
            case DATE:
                conditionSatisfied = compareDate(inputTuple);
                break;
            case DATETIME:
                conditionSatisfied = compareDateTime(inputTuple);
                break;
            case DOUBLE:
                conditionSatisfied = compareDouble(inputTuple);
                break;
            case INTEGER:
                conditionSatisfied = compareInt(inputTuple);
                break;
            case STRING:
            case _ID_TYPE:
                conditionSatisfied = compareString(inputTuple);
                break;
            case TEXT:
                conditionSatisfied = compareText(inputTuple);
                break;
            case LIST:
                throw new DataflowException("Unable to do comparison: LIST type is not supported");
            default:
                throw new DataflowException("Unable to do comparison: unknown type " + inputAttrType.getName());
        }
        return conditionSatisfied ? inputTuple : null;
    }

    private boolean compareDate(Tuple inputTuple) throws DataflowException {     
        LocalDate date = inputTuple.getField(predicate.getAttributeName(), DateField.class).getValue();
        String compareToString = predicate.getCompareToValue();
        
        // try to parse the input as date string first
        try {
            LocalDate compareToDate = LocalDate.parse(compareToString);
            return compareValues(date, compareToDate, predicate.getComparisonType());
        } catch (DateTimeParseException e) {
            // if it fails, then try to parse as date time string 
            try {
                LocalDateTime compareToDateTime = LocalDateTime.parse(compareToString);
                return compareValues(date, compareToDateTime.toLocalDate(), predicate.getComparisonType());
            } catch ( DateTimeParseException e2) {
                throw new DataflowException("Unable to parse date or time: " + compareToString);
            }
        }
    }
    
    private boolean compareDateTime(Tuple inputTuple) throws DataflowException {
        LocalDateTime dateTime = inputTuple.getField(predicate.getAttributeName(), DateTimeField.class).getValue();
        String compareToString = predicate.getCompareToValue();
        
        // try to parse the input as date time string first
        try {
            LocalDateTime compareToDateTime = LocalDateTime.parse(compareToString);
            return compareValues(dateTime, compareToDateTime, predicate.getComparisonType());
        } catch (DateTimeParseException e) {
            // if it fails, then try to parse as date time string and compare on date
            try {
                LocalDate compareToDate = LocalDate.parse(compareToString);
                return compareValues(dateTime.toLocalDate(), compareToDate, predicate.getComparisonType());
            } catch ( DateTimeParseException e2) {
                throw new DataflowException("Unable to parse date or time: " + compareToString);
            }
        }
    }

    private boolean compareDouble(Tuple inputTuple) {
        Double value = inputTuple.getField(predicate.getAttributeName(), DoubleField.class).getValue();
        try {
            Double compareToValue = Double.parseDouble( predicate.getCompareToValue());
            return compareValues(value, compareToValue, predicate.getComparisonType());
        } catch (NumberFormatException e) {
            throw new DataflowException("Unable to parse to number " + e.getMessage());
        }
    }

    private boolean compareInt(Tuple inputTuple) {
        Integer value = inputTuple.getField(predicate.getAttributeName(), IntegerField.class).getValue();
        try {
            Double compareToValue = Double.parseDouble(predicate.getCompareToValue());
            return compareValues((double) value, compareToValue, predicate.getComparisonType());
        } catch (NumberFormatException e) {
            throw new DataflowException("Unable to parse to number " + e.getMessage());
        }
    }

    private boolean compareString(Tuple inputTuple) {
        return compareValues(
                inputTuple.getField(predicate.getAttributeName(), StringField.class).getValue(),
                predicate.getCompareToValue(), predicate.getComparisonType());
    }

    private boolean compareText(Tuple inputTuple) {
        return compareValues(
                inputTuple.getField(predicate.getAttributeName(), TextField.class).getValue(),
                predicate.getCompareToValue(), predicate.getComparisonType());
    }

    private static <T extends Comparable<T>> boolean compareValues(T value, T compareToValue, ComparisonType comparisonType) {
        int compareResult = value.compareTo(compareToValue);
        switch (comparisonType) {
        case EQUAL_TO:
            if (compareResult == 0) {
                return true;
            }
            break;
        case GREATER_THAN:
            if (compareResult > 0) {
                return true;
            }
            break;
        case GREATER_THAN_OR_EQUAL_TO:
            if (compareResult >= 0) {
                return true;
            }
            break;
        case LESS_THAN:
            if (compareResult < 0) {
                return true;
            }
            break;
        case LESS_THAN_OR_EQUAL_TO:
            if (compareResult <= 0) {
                return true;
            }
            break;
        case NOT_EQUAL_TO:
            if (compareResult != 0) {
                return true;
            }
            break;
        default:
            throw new DataflowException(
                    "Unable to do comparison: unknown comparison type: " + comparisonType);
        }
        return false;
    }

    @Override
    protected void cleanUp() throws DataflowException {
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema output = inputSchema[0];
        if (! output.containsAttribute(predicate.getAttributeName())) {
            throw new DataflowException(String.format("attribute %s not contained in input schema %s",
                predicate.getAttributeName(), output.getAttributeNames()));
        }
        return output;
    }

}
