package edu.uci.ics.texera.workflow.common.metadata.annotations;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@JacksonAnnotationsInside
@JsonSchemaInject(json = "{" +
                         "  \"attributeType1\": {\n" +
                         "    \"attribute\":{\n" +
                         "      \"enum\": [\"string\"]\n" +
                         "    }\n" +
                         "  }\n" +
                         "}"
)
public @interface SentimentAnalysisAttributeTypeSchema {
}
