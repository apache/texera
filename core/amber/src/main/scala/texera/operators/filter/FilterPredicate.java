package texera.operators.filter;

import Engine.Common.tuple.Tuple;
import Engine.Common.tuple.texera.TexeraTuple;
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
    public boolean evaluate(TexeraTuple tuple, TexeraContext context) {
        String tupleValue = tuple.getField(attribute).toString().trim();
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
