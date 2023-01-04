package edu.uci.ics.texera.workflow.operators.udf.pythonV2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;

import java.util.Objects;

public class NewAttributeUnit {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute Name")
    public String attributeName;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute Type")
    public AttributeType attributeType;

    @JsonProperty
    @JsonSchemaTitle("Expression")
    @JsonPropertyDescription("Type the lambda expression. To specify an existing column, wrap the column name in backticks(`), e.g. `Unit Price`")
    public String expression;

    NewAttributeUnit(String attributeName, AttributeType attributeType, String expression) {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
        this.expression = expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewAttributeUnit that = (NewAttributeUnit) o;
        return Objects.equals(attributeName, that.attributeName) && Objects.equals(attributeType, that.attributeType)
                && Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, attributeType, expression);
    }
}
