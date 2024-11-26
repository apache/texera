package edu.uci.ics.amber.operator.udf.python;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaBool;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.core.tuple.AttributeType;
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeNameLambda;
import edu.uci.ics.amber.operator.metadata.annotations.HideAnnotation;

import java.util.Objects;

public class LambdaAttributeUnit {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute Name")
    @AutofillAttributeNameLambda
    public String attributeName;

    @JsonProperty
    @JsonSchemaTitle("New Attribute Name")
    @JsonSchemaInject(
            strings = {
                    @JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeName"),
                    @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
                    @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "(?!Add New Column).*")
            },
            bools = @JsonSchemaBool(path = HideAnnotation.hideOnNull, value = true)
    )
    public String newAttributeName;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute Type")
    public AttributeType attributeType;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Expression")
    public String expression;

    LambdaAttributeUnit(String attributeName, String expression, String newAttributeName, AttributeType newAttributeType) {
        this.attributeName = attributeName;
        this.expression = expression;
        this.newAttributeName = newAttributeName;
        this.attributeType = newAttributeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LambdaAttributeUnit that = (LambdaAttributeUnit) o;
        return Objects.equals(attributeName, that.attributeName) &&
                Objects.equals(expression, that.expression) &&
                Objects.equals(newAttributeName, that.newAttributeName) &&
                Objects.equals(attributeType, that.attributeType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, expression, newAttributeName, attributeType);
    }
}
