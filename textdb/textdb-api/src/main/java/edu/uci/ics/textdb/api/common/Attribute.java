package edu.uci.ics.textdb.api.common;

public class Attribute {
    private final String fieldName;
    private final FieldType fieldType;

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
    public boolean equals(Object toCompare) {
        if (this == toCompare) {
            return true;
        }
        if (toCompare == null) {
            return false;
        }
        if (this.getClass() != toCompare.getClass()) {
            return false;
        }
        
        Attribute that = (Attribute) toCompare;
        
        if (this.fieldName == null) {
            return that.fieldName == null;
        }
        if (this.fieldType == null) {
            return that.fieldType == null;
        }
        
        return this.fieldName.equals(that.fieldName) && this.fieldType.equals(that.fieldType);
    }
    
    @Override
    public int hashCode() {
        return this.fieldName.hashCode() + this.fieldType.toString().hashCode();
    }
}
