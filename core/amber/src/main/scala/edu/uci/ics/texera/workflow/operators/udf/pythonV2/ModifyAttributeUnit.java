package edu.uci.ics.texera.workflow.operators.udf.pythonV2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;

import java.util.Objects;

public class ModifyAttributeUnit {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute Name")
    @AutofillAttributeName
    public String attributeName;

    @JsonProperty
    @JsonSchemaTitle("Expression")
    @JsonPropertyDescription("Type the lambda expression. To specify an existing column, wrap the column name in backticks(`), e.g. `Unit Price`")
    public String expression;

    ModifyAttributeUnit(String attributeName, String expression) {
        this.attributeName = attributeName;
        this.expression = expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModifyAttributeUnit that = (ModifyAttributeUnit) o;
        return Objects.equals(attributeName, that.attributeName) && Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, expression);
    }
}
