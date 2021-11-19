package edu.uci.ics.texera.workflow.common.metadata.annotations;

import java.lang.annotation.*;
import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaBool;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@JacksonAnnotationsInside
@JsonSchemaInject(
        bools = @JsonSchemaBool(path = "enable-presets", value = true))
public @interface EnablePresets {
    String path = "enable-presets";
    boolean value = true;
}