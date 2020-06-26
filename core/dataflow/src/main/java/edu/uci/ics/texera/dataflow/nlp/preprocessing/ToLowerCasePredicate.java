package edu.uci.ics.texera.dataflow.nlp.preprocessing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import java.util.Map;

public class ToLowerCasePredicate extends PredicateBase {
    private final String inputAttributeName;
    private final String resultAttributeName;
    @JsonCreator
    public ToLowerCasePredicate(
        @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
        String attribute,
        @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
        String resultAttribute
    ) {
        this.inputAttributeName = attribute;
        this.resultAttributeName = resultAttribute;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }

    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() { return this.resultAttributeName; }

    @Override
    public ToLowerCaseOperator newOperator() {
        return new ToLowerCaseOperator(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Lower Case")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Convert the upper case English character into lower case character in the String.")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SPLIT_GROUP)
            .build();
    }

}
