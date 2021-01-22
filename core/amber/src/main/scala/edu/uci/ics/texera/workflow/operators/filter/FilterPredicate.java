package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.texera.workflow.common.WorkflowContext;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;

public class FilterPredicate {

    @JsonProperty(value = "attribute", required = true)
    @AutofillAttributeName
    public String attribute;

    @JsonProperty(value = "condition", required = true)
    public ComparisonType condition;

    @JsonProperty(value = "value", required = true)
    public String value;


    @JsonIgnore
    public boolean evaluate(Tuple tuple, WorkflowContext context) {
        AttributeType type = tuple.getSchema().getAttribute(this.attribute).getType();
        switch (type) {
            case STRING:
            case ANY:
                return evaluateFilterString(tuple);
            case INTEGER:
                return evaluateFilterInt(tuple);
            case DOUBLE:
                return evaluateFilterDouble(tuple);
            case BOOLEAN:
                return evaluateFilterBoolean(tuple);
            default:
                throw new RuntimeException("unsupported attribute type: " + type);
        }
    }


    private boolean evaluateFilterDouble(Tuple inputTuple) {
        Double tupleValue = inputTuple.getField(this.attribute, Double.class);
        Double compareToValue = Double.parseDouble(this.value);
        return evaluateFilter(tupleValue, compareToValue, this.condition);
    }

    private boolean evaluateFilterInt(Tuple inputTuple) {
        Integer tupleValueInt = inputTuple.getField(this.attribute, Integer.class);
        Double tupleValueDouble = tupleValueInt == null ? null : (double) tupleValueInt;
        Double compareToValue = Double.parseDouble(this.value);
        return evaluateFilter(tupleValueDouble, compareToValue, this.condition);
    }

    private boolean evaluateFilterString(Tuple inputTuple) {
        String tupleValue = inputTuple.getField(this.attribute).toString();
        try {
            Double tupleValueDouble = tupleValue == null ? null : Double.parseDouble(tupleValue.trim());
            Double compareToValueDouble = Double.parseDouble(this.value);
            return evaluateFilter(tupleValueDouble, compareToValueDouble, this.condition);
        } catch (NumberFormatException e) {
            return evaluateFilter(tupleValue, this.value, this.condition);
        }
    }

    private boolean evaluateFilterBoolean(Tuple inputTuple) {
        Boolean tupleValue = inputTuple.getField(this.attribute, Boolean.class);
        return evaluateFilter(tupleValue.toString().toLowerCase(), this.value.trim().toLowerCase(), this.condition);
    }

    private static <T extends Comparable<T>> boolean evaluateFilter(T value, T compareToValue, ComparisonType comparisonType) {
        if (value == null) {
            return compareToValue == null;
        }
        int compareResult = value.compareTo(compareToValue);
        switch (comparisonType) {
            case EQUAL_TO:
                return compareResult == 0;
            case GREATER_THAN:
                return compareResult > 0;
            case GREATER_THAN_OR_EQUAL_TO:
                return compareResult >= 0;
            case LESS_THAN:
                return compareResult < 0;
            case LESS_THAN_OR_EQUAL_TO:
                return compareResult <= 0;
            case NOT_EQUAL_TO:
                return compareResult != 0;
            default:
                throw new RuntimeException(
                        "Unable to do comparison: unknown comparison type: " + comparisonType);
        }
    }

}
