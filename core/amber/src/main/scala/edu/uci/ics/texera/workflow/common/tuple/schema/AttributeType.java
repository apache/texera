package edu.uci.ics.texera.workflow.common.tuple.schema;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.sql.Timestamp;

public enum AttributeType implements Serializable {

    /**
     * To add a new AttributeType, update the following files to handle the new type:
     * 1. SourceOp:
     * Especially SQLSources will need to map the input schema to Texera.Schema. AttributeType
     * needs to be converted from original source types accordingly.
     * <p>
     * 2. FilterPredicate:
     * FilterPredicate takes in AttributeTypes and converts them into a comparable type, then do
     * the comparison. New AttributeTypes needs to be mapped to a comparable type there.
     * <p>
     * 3. SpecializedAverageOpDesc.getNumericalValue:
     * New AttributeTypes might need to be converted into a numerical value in order to perform
     * aggregations.
     * <p>
     * 4. SchemaPropagationService.SchemaAttribute (frontend).
     * <p>
     * 5. TypeCastingOpExec (optional as this stage):
     * Typical type casting among internal types, if added also to TypeCastingAttributeType.
     */


    // A field that is indexed but not tokenized: the entire String
    // value is indexed as a single token
    STRING("string", String.class),
    INTEGER("integer", Integer.class),
    LONG("long", Long.class),
    DOUBLE("double", Double.class),
    BOOLEAN("boolean", Boolean.class),
    TIMESTAMP("timestamp", Timestamp.class),
    ANY("ANY", Object.class);

    private final String name;
    private final Class<?> fieldClass;

    AttributeType(String name, Class<?> fieldClass) {
        this.name = name;
        this.fieldClass = fieldClass;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    public Class<?> getFieldClass() {
        return this.fieldClass;
    }

    public static AttributeType getAttributeType(Class<?> fieldClass) {
        if (fieldClass.equals(String.class)) {
            return STRING;
        } else if (fieldClass.equals(Integer.class)) {
            return INTEGER;
        } else if (fieldClass.equals(Long.class)) {
            return LONG;
        } else if (fieldClass.equals(Double.class)) {
            return DOUBLE;
        } else if (fieldClass.equals(Boolean.class)) {
            return BOOLEAN;
        } else if (fieldClass.equals(Timestamp.class)) {
            return TIMESTAMP;
        } else {
            return ANY;
        }
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
