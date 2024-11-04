package edu.uci.ics.amber.operator.common.annotations;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@JacksonAnnotationsInside
@JsonSchemaInject(
        strings = {
                @JsonSchemaString(path = CommonOpDescAnnotation.autofill, value = CommonOpDescAnnotation.attributeName),
                @JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeName"),
                @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
                @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "Add New Column"),
                @JsonSchemaString(path = "additionalEnumValue", value = "Add New Column"),
        },
        ints = @JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 0))
public @interface AutofillAttributeNameLambda {

}
