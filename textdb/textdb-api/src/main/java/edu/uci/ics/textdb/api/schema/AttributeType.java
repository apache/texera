package edu.uci.ics.textdb.api.schema;

import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;

public enum AttributeType {
    // A field that is indexed but not tokenized: the entire String
    // value is indexed as a single token
    STRING("string", StringField.class), 
    // A field that is indexed and tokenized,without term vectors
    TEXT("text", TextField.class),
    INTEGER("integer", IntegerField.class), 
    DOUBLE("double", DoubleField.class), 
    DATE("date", DateField.class),

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
}
