package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.texera.workflow.common.WorkflowContext;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;

import java.sql.Timestamp;

public class FilterPredicate {

    @JsonProperty(value = "attribute", required = true)
    @AutofillAttributeName
    public String attribute;

    @JsonProperty(value = "condition", required = true)
    public ComparisonType condition;

    @JsonProperty(value = "value")
    public String value;

    //Add a constructor for test sample
    public FilterPredicate(String a, ComparisonType ct, String v){
        this.attribute=a;
        this.condition=ct;
        this.value=v;
    }
    @JsonIgnore
    public boolean evaluate(Tuple tuple, WorkflowContext context) {
        AttributeType type = tuple.getSchema().getAttribute(this.attribute).getType();
        switch (type) {
            case STRING:
                return evaluateFilterString(tuple);
            case ANY:
                return evaluateFilterString(tuple);
            case BOOLEAN:
                return evaluateFilterBoolean(tuple);
            case LONG:
                return evaluateFilterLong(tuple);
            case INTEGER:
                return evaluateFilterInt(tuple);
            case DOUBLE:
                return evaluateFilterDouble(tuple);
            case TIMESTAMP:
                return evaluateFilterTimestamp(tuple);

            default:
                throw new RuntimeException("unsupported attribute type: " + type);
        }
    }

    private boolean evaluateFilterBoolean(Tuple inputTuple) {
        Boolean tupleValue = inputTuple.getField(attribute, Boolean.class);
        return evaluateFilter(tupleValue.toString().toLowerCase(), value.trim().toLowerCase(), condition);
    }

    private boolean evaluateFilterDouble(Tuple inputTuple) {
        Double tupleValue = inputTuple.getField(attribute, Double.class);
        Double compareToValue = Double.parseDouble(value);
        return evaluateFilter(tupleValue, compareToValue, condition);
    }

    private boolean evaluateFilterInt(Tuple inputTuple) {
        Integer tupleValueInt = inputTuple.getField(attribute, Integer.class);
        Double tupleValueDouble = tupleValueInt == null ? null : (double) tupleValueInt;
        Double compareToValue = Double.parseDouble(value);
        return evaluateFilter(tupleValueDouble, compareToValue, condition);
    }

    private boolean evaluateFilterLong(Tuple inputTuple) {
        Long tupleValue = inputTuple.getField(attribute, Long.class);
        Long compareToValue = Long.valueOf(value.trim());
        return evaluateFilter(tupleValue, compareToValue, condition);
    }

    private boolean evaluateFilterString(Tuple inputTuple) {
        String tupleValue = inputTuple.getField(attribute, String.class);
        return evaluateFilter(tupleValue,value.trim(),condition);
        /*if (tupleValue==null){
            return evaluateFilter(tupleValue, "1", condition);
        }
        else {
            try {
                Double tupleValueDouble = Double.parseDouble(tupleValue.trim());
                Double compareToValueDouble = Double.parseDouble(value);
                return evaluateFilter(tupleValueDouble, compareToValueDouble, condition);
            } catch (NumberFormatException e) {
                return false;
            }
        } */

    }


    private boolean evaluateFilterTimestamp(Tuple inputTuple) {
        Long tupleValue = inputTuple.getField(attribute, Timestamp.class).getTime();
        Long compareToValue = AttributeTypeUtils.parseTimestamp(value.trim()).getTime();
        return evaluateFilter(tupleValue, compareToValue, condition);

    }




    private static <T extends Comparable<T>> boolean evaluateFilter(T value, T compareToValue, ComparisonType comparisonType) {
        int compareResult;
        switch (comparisonType) {
            case NOT_NULL:
                if(value==null){
                    return false;
                }
                else{
                    return true;
                }

            case IS_NULL:
                if(value==null){
                    return true;
                }
                else{
                    return false;
                }

            case EQUAL_TO:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult == 0;
                }
            case GREATER_THAN:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult > 0;
                }
            case GREATER_THAN_OR_EQUAL_TO:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult >= 0;
                }
            case LESS_THAN:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult < 0;
                }
            case LESS_THAN_OR_EQUAL_TO:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult <= 0;
                }
            case NOT_EQUAL_TO:
                if(value==null){
                    return false;
                }
                else{
                    compareResult = value.compareTo(compareToValue);
                    return compareResult != 0;
                }
            default:
                throw new RuntimeException(
                        "Unable to do comparison: unknown comparison type: " + comparisonType);
        }
    }

}
