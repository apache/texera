package edu.uci.ics.texera.api.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;

/**
 * An attribute describes the name and the type of a column.
 * 
 * @author zuozhiw
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
        checkNotNull(attributeName);
        checkNotNull(attributeType);
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }
    
    @JsonProperty(value = JsonConstants.ATTRIBUTE_NAME)
    public String getName() {
        return attributeName;
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTE_TYPE)
    public AttributeType getType() {
        return attributeType;
    }

    @Override
    public String toString() {
        return "Attribute[name=" + attributeName + ", type=" + attributeType + "]";
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
        
        return this.attributeName.equalsIgnoreCase(that.attributeName) && this.attributeType.equals(that.attributeType);
    }
    
    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.attributeType.toString().hashCode();
    }
}
