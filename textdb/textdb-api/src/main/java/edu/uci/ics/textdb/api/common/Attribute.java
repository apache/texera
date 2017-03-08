package edu.uci.ics.textdb.api.common;

public class Attribute {
    private final String attributeName;
    private final FieldType fieldType;

    public Attribute(String attributeName, FieldType type) {
        this.attributeName = attributeName;
        this.fieldType = type;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public String getAttributeName() {
        return attributeName;
    }

    @Override
    public String toString() {
        return "Attribute [attributeName=" + attributeName + ", fieldType=" + fieldType + "]";
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
        
        if (this.attributeName == null) {
            return that.attributeName == null;
        }
        if (this.fieldType == null) {
            return that.fieldType == null;
        }
        
        return this.attributeName.equals(that.attributeName) && this.fieldType.equals(that.fieldType);
    }
    
    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.fieldType.toString().hashCode();
    }
}
