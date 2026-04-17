/*
 * Copyright (c) 2016 Kjell Tore Eliassen (mbknor)
 * Licensed under the MIT License.
 *
 * This file is derived from mbknor-jackson-jsonschema.
 * Source: https://github.com/mbknor/mbknor-jackson-jsonschema
 */


package com.kjetland.jackson.jsonSchema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
public @interface JsonSchemaArrayWithUniqueItems {
    String value();
}
