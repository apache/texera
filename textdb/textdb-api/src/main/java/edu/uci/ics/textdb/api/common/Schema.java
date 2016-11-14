package edu.uci.ics.textdb.api.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Schema {
    private List<Attribute> attributes;
    private Map<String, Integer> fieldNameVsIndex;

    public Schema(Attribute... attributes) {
        // Converting to java.util.Arrays.ArrayList
        // so that the collection remains static and cannot be extended/shrunk
        // This makes List<Attribute> partially immutable.
        // Partial because we can still replace an element at particular index.
        this.attributes = Arrays.asList(attributes);
        populateFieldNameVsIndexMap();
    }

    private void populateFieldNameVsIndexMap() {
        fieldNameVsIndex = new HashMap<String, Integer>();
        for (int count = 0; count < attributes.size(); count++) {
            String fieldName = attributes.get(count).getFieldName();
            fieldNameVsIndex.put(fieldName.toLowerCase(), count);
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }
    
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getFieldName()).collect(Collectors.toList());
    }

    public Integer getIndex(String fieldName) {
        return fieldNameVsIndex.get(fieldName.toLowerCase());
    }
    
    public Attribute getAttribute(String fieldName) {
        Integer attrIndex = getIndex(fieldName);
        if (attrIndex == null) {
            return null;
        }
        return attributes.get(attrIndex);
    }

    public boolean containsField(String fieldName) {
        return fieldNameVsIndex.keySet().contains(fieldName.toLowerCase());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((fieldNameVsIndex == null) ? 0 : fieldNameVsIndex.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Schema other = (Schema) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        if (fieldNameVsIndex == null) {
            if (other.fieldNameVsIndex != null)
                return false;
        } else if (!fieldNameVsIndex.equals(other.fieldNameVsIndex))
            return false;
        return true;
    }
}
