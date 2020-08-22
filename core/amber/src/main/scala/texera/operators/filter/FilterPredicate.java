package texera.operators.filter;

import Engine.Common.AmberTuple.Tuple;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import texera.common.TexeraContext;

public class FilterPredicate {

    @JsonProperty("attribute")
    public String attribute;

    @JsonProperty("condition")
    public ComparisonType condition;

    @JsonProperty("value")
    public String value;


    @JsonIgnore
    public boolean evaluate(Tuple tuple, TexeraContext context) {
        Integer field = context.fieldIndexMapping(attribute);
        String tupleValue = tuple.get(field).toString().trim();
        switch (condition) {
            case EQUAL_TO:
                return tupleValue.equalsIgnoreCase(value);
            case GREATER_THAN:
                return tupleValue.compareToIgnoreCase(value) > 0;
            case GREATER_THAN_OR_EQUAL_TO:
                return tupleValue.compareToIgnoreCase(value) >= 0;
            case LESS_THAN:
                return tupleValue.compareToIgnoreCase(value) < 0;
            case LESS_THAN_OR_EQUAL_TO:
                return tupleValue.compareToIgnoreCase(value) <= 0;
            case NOT_EQUAL_TO:
                return ! tupleValue.equalsIgnoreCase(value);
        }
        return false;
    }

}
