package com.kjetland.jackson.jsonSchema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Injects custom values to the schema generated for fields or getters.
 *
 * @author bbyk
 */
@Target({METHOD, FIELD, TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonSchemaInt {
    /**
     * @return a slash separated path to the value in the schema
     */
    String path();

    /**
     * @return an int value to place in the schema
     */
    int value();
}
