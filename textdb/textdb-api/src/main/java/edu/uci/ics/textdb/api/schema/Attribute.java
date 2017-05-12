package edu.uci.ics.textdb.api.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;

/**
 * An Attribute describes the name and type of a "column".
 * 
 * The name of the attribute will always be trimmed and is case insensitive.
 * 
 * @author Zuozhi Wang
 *
 */
public class Attribute {
    private final String attributeName;
    private final AttributeType attributeType;

    @JsonCreator
    public Attribute(
            @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME, required = true)
            String attributeName, 
            @JsonProperty(value = JsonConstants.ATTRIBUTE_TYPE, required = true)
            AttributeType attributeType) {     
        if (attributeName == null) {
            throw new TextDBException("Cannot create Attribute: attributeName is null");
        }
        if (attributeType == null) {
            throw new TextDBException("Cannot create Attribute: attributeType is null");
        }
        if (attributeName.trim().isEmpty()) {
            throw new TextDBException("Cannot create Attribute: attributeName is empty");
        }
        this.attributeName = attributeName.trim();
        this.attributeType = attributeType;
    }
    
    @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTE_TYPE)
    public AttributeType getAttributeType() {
        return attributeType;
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
        return this.attributeName.equals(that.attributeName) && this.attributeType.equals(that.attributeType);
    }
    
    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.attributeType.toString().hashCode();
    }
}
