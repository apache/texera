package edu.uci.ics.texera.workflow.common.metadata.annotations;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaBool;
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
        bools = @JsonSchemaBool(path = CommonOpDescAnnotation.autofill, value = true))
public @interface SkipSchema {

}
