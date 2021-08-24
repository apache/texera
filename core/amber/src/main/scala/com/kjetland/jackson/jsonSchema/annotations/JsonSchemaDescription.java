package com.kjetland.jackson.jsonSchema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Same as com.fasterxml.jackson.annotation.JsonPropertyDescription
 */
@Target({ METHOD, FIELD, PARAMETER, TYPE })
@Retention(RUNTIME)
public @interface JsonSchemaDescription {
    String value();
}
