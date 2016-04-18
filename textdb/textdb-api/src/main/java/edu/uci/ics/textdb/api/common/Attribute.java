package edu.uci.ics.textdb.api.common;

public class Attribute {
    private String fieldName;
    private FieldType fieldType;
    
    public Attribute(String fieldName, FieldType type){
        this.fieldName = fieldName;
        this.fieldType = type;
    }
    
    public FieldType getFieldType() {
        return fieldType;
    }
    
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "Attribute [fieldName=" + fieldName + ", fieldType=" + fieldType
                + "]";
    }
}
