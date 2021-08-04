package com.kjetland.jackson.jsonSchema.annotations;

import com.fasterxml.jackson.databind.JsonNode;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Use this annotation to inject json into the generated jsonSchema.
 */
@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
public @interface JsonSchemaInject {
    /**
     * @return a raw json that will be merged on top of the generated jsonSchema
     */
    String json() default "{}";

    /**
     * @return a class for supplier of a raw json. The json gets applied after {@link #json()}.
     */
    Class<? extends Supplier<JsonNode>> jsonSupplier() default None.class;

    /**
     * @return a key to lookup a jsonSupplier via lookupMap defined in JsonSchemaConfig
     */
    String jsonSupplierViaLookup() default "";

    /**
     * @return a collection of key/value pairs to merge on top of the generated jsonSchema and applied after {@link #jsonSupplier()}
     */
    JsonSchemaString[] strings() default {};

    /**
     * @return a collection of key/value pairs to merge on top of the generated jsonSchema and applied after {@link #jsonSupplier()
     */
    JsonSchemaInt[] ints() default {};

    /**
     * @return a collection of key/value pairs to merge on top of the generated jsonSchema and applied after {@link #jsonSupplier()
     */
    JsonSchemaBool[] bools() default {};

    /**
     * If merge is true (the default), the injected json will be injected into the generated jsonSchema-node. If merge = false, then
     * we skips the generated jsonSchema-node and use the entire injected one instead.
     * @return whether we should merge or replaceWith the injected json
     */
    boolean merge() default true;

    // This can be used in the same way as 'groups' in javax.validation.constraints, e.g @NotNull
    Class<?>[] javaxValidationGroups() default { };

    class None implements Supplier<JsonNode> {
        @Override
        public JsonNode get() {
            return null;
        }
    }
}

