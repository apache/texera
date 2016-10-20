package edu.uci.ics.textdb.api.common;

public class Attribute {
    private String fieldName;
    private FieldType fieldType;

    public Attribute(String fieldName, FieldType type) {
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
        return "Attribute [fieldName=" + fieldName + ", fieldType=" + fieldType + "]";
    }
    
    @Override
    public int hashCode() {
        return fieldName.hashCode() + fieldType.toString().hashCode();
    }
    
    @Override
    public boolean equals(Object toCompare) {
        if (this == toCompare) {
            return true;
        }
        if (toCompare == null) {
            return false;
        }
        if (! (toCompare instanceof Attribute)) {
            return false;
        }
        Attribute that = (Attribute) toCompare;
        return this.fieldName.equals(that.fieldName) && this.fieldType.equals(that.fieldType);
    }
}
