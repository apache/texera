package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaBool;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.operator.common.annotations.HideAnnotation;
import edu.uci.ics.amber.operator.common.annotations.CommonOpDescAnnotation;


public class HyperParameters<T>{
    @JsonProperty(required = true)
    @JsonSchemaTitle("Parameter")
    @JsonPropertyDescription("Choose the name of the parameter")
    public T parameter;

    @JsonSchemaInject(
            strings = {
                    @JsonSchemaString(path = CommonOpDescAnnotation.autofill, value = CommonOpDescAnnotation.attributeName),
                    @JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
                    @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
                    @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
            },
            ints = @JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 1))
    @JsonProperty(value = "attribute")
    public String attribute;

    @JsonSchemaInject(strings = {
            @JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
            @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
            @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    },
            bools = @JsonSchemaBool(path = HideAnnotation.hideOnNull, value = true))
    @JsonProperty(value = "value")
    public String value;

    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Workflow")
    @JsonPropertyDescription("Parameter from workflow")
    public boolean parametersSource;
}
