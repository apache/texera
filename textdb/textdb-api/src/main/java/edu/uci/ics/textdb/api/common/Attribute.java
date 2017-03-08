package edu.uci.ics.textdb.api.common;

public class Attribute {
    private final String attributeName;
    private final FieldType attributeType;

    public Attribute(String attributeName, FieldType type) {
        this.attributeName = attributeName;
        this.attributeType = type;
    }

    public FieldType getAttributeType() {
        return attributeType;
    }

    public String getAttributeName() {
        return attributeName;
    }

    @Override
    public String toString() {
        return "Attribute [attributeName=" + attributeName + ", attributeType=" + attributeType + "]";
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
        if (this.attributeType == null) {
            return that.attributeType == null;
        }
        
        return this.attributeName.equals(that.attributeName) && this.attributeType.equals(that.attributeType);
    }
    
    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.attributeType.toString().hashCode();
    }
}
