package Engine.SchemaSupport.schema;

import com.fasterxml.jackson.annotation.JsonValue;
import Engine.SchemaSupport.exception.AmberException;
import Engine.SchemaSupport.field.*;


public enum AttributeType {
    // A field that is indexed but not tokenized: the entire String
    // value is indexed as a single token
    STRING("string", StringField.class),
    // A field that is indexed and tokenized,without term vectors
    TEXT("text", TextField.class),
    INTEGER("integer", IntegerField.class),
    DOUBLE("double", DoubleField.class),
    DATE("date", DateField.class),
    DATETIME("datetime", DateTimeField.class),
    BOOLEAN("boolean", StringField.class),

    _ID_TYPE("_id", IDField.class),
    // A field that is the list of values
    LIST("list", ListField.class);
    
    private String name;
    private Class<? extends IField> fieldClass;
    
    AttributeType(String name, Class<? extends IField> fieldClass) {
        this.name = name;
        this.fieldClass = fieldClass;
    }
    
    @JsonValue
    public String getName() {
        return this.name;
    }
    
    public Class<? extends IField> getFieldClass() {
        return this.fieldClass;
    }
    
    public static AttributeType getAttributeType(Class<? extends IField> fieldClass) {
        if (fieldClass.equals(StringField.class)) {
            return STRING;
        } else if (fieldClass.equals(TextField.class)) {
            return TEXT;
        } else if (fieldClass.equals(IntegerField.class)) {
            return INTEGER;
        } else if (fieldClass.equals(DoubleField.class)) {
            return DOUBLE;
        } else if (fieldClass.equals(DateField.class)) {
            return DATE;
        } else if (fieldClass.equals(IDField.class)) {
            return _ID_TYPE;
        } else if (fieldClass.equals(ListField.class)) {
            return LIST;
        } else {
            throw new AmberException("Unkown IField class: " + fieldClass.getName());
        }
    }
    
    @Override
    public String toString() {
        return this.getName();
    }
}
