package edu.uci.ics.texera.workflow.common.metadata.annotations;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

import java.lang.annotation.*;

import static edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@JacksonAnnotationsInside
@JsonSchemaInject(
        strings = @JsonSchemaString(path = autofill, value = attributeName),
        ints = @JsonSchemaInt(path = autofillAttributeOnPort, value = 0))
public @interface AutofillAttributeName {

    // JSON schema key
    String autofill = "autofill";

    // allowed JSON schema values for the key autoCompleteType
    String attributeName = "attributeName";
    String attributeNameList = "attributeNameList";

    // JSON schema key to indicate which port
    String autofillAttributeOnPort = "autofillAttributeOnPort";

}
