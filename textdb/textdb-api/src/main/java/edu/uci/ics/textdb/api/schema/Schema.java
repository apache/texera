package edu.uci.ics.textdb.api.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;

public class Schema {
    
    private final List<Attribute> attributes;
    private final Map<String, Integer> nameLowercaseIndexMap;

    public Schema(Attribute... attributes) {
        this(Arrays.asList(attributes));
    }
    
    @JsonCreator
    public Schema(
            @JsonProperty(value = JsonConstants.ATTRIBUTES, required = true)
            List<Attribute> attributes) {
        if (attributes.isEmpty()) {
            throw new TextDBException("Cannot create schema: attributes is empty");
        }
        this.attributes = new ArrayList<>(attributes);
        this.nameLowercaseIndexMap = new HashMap<>();
        
        for (int i = 0; i < attributes.size(); i++) {
            String attributeNameLowercase = attributes.get(i).getAttributeName().toLowerCase();
            if (nameLowercaseIndexMap.containsKey(attributeNameLowercase)) {
                throw new TextDBException("Cannot create schema: duplicate attribute name " + attributeNameLowercase + 
                        ". (attribute name is case insensitive)");
            }
            nameLowercaseIndexMap.put(attributeNameLowercase, i);
        }
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTES)
    public List<Attribute> getAttributes() {
        return new ArrayList<>(attributes);
    }
    
    @JsonIgnore
    public int size() {
        return attributes.size();
    }
    
    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getAttributeName()).collect(Collectors.toList());
    }

    public Integer getIndex(String attributeName) {
        String attributeNameLowercase = attributeName.trim().toLowerCase();
        if (! nameLowercaseIndexMap.containsKey(attributeNameLowercase)) {
            throw new TextDBException(String.format(
                    "Cannot find the attribute: %s does not exist in the schema %s", 
                    attributeName, this.getAttributeNames()));
        }
        return nameLowercaseIndexMap.get(attributeNameLowercase);
    }
    
    public Attribute getAttribute(String attributeName) {
        return attributes.get(getIndex(attributeName));
    }
    
    public Attribute getAttribute(int index) {
        if (index < attributes.size()) {
            return attributes.get(index);
        }
        throw new TextDBException(String.format(
                "Cannot get attribute at index %d: schema only has %d attributes", 
                index, attributes.size()));
    }

    @JsonIgnore
    public boolean containsField(String attributeName) {
        String attributeNameLowercase = attributeName.trim().toLowerCase();
        return nameLowercaseIndexMap.containsKey(attributeNameLowercase);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((nameLowercaseIndexMap == null) ? 0 : nameLowercaseIndexMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object toCompare) {
        if (this == toCompare)
            return true;
        if (toCompare == null)
            return false;
        if (getClass() != toCompare.getClass())
            return false;
        
        Schema that = (Schema) toCompare;  
        return this.attributes.equals(that.attributes) 
                && this.nameLowercaseIndexMap.equals(that.nameLowercaseIndexMap);
    }
}
