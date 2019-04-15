package edu.uci.ics.texera.api.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;

/**
 * A schema is a list of attributes that describe all the columns of a table.
 * 
 * @author zuozhiw
 *
 */
public class Schema {
    private final ImmutableList<Attribute> attributes;
    private final ImmutableMap<String, Integer> attributeIndex;

    public Schema(Attribute... attributes) {
        this(Arrays.asList(attributes));
    }
    
    @JsonCreator
    public Schema(
            @JsonProperty(value = JsonConstants.ATTRIBUTES, required = true)
            List<Attribute> attributes) {
        checkNotNull(attributes);
        this.attributes = ImmutableList.copyOf(attributes);
        HashMap<String, Integer> attributeIndexTemp = new HashMap<String, Integer>();
        for (int i = 0; i < attributes.size(); i++) {
            attributeIndexTemp.put(attributes.get(i).getName().toLowerCase(), i);
        }
        this.attributeIndex = ImmutableMap.copyOf(attributeIndexTemp);
    }

    @JsonProperty(value = JsonConstants.ATTRIBUTES)
    public List<Attribute> getAttributes() {
        return attributes;
    }
    
    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getName()).collect(Collectors.toList());
    }

    public Integer getIndex(String attributeName) {
	    	if (! containsAttribute(attributeName)) {
	    		throw new TexeraException(attributeName + " is not contained in the schema");
	    	}
	    return attributeIndex.get(attributeName.toLowerCase());
    }
    
    public Attribute getAttribute(String attributeName) {
        return attributes.get(getIndex(attributeName));
    }

    @JsonIgnore
    public boolean containsAttribute(String attributeName) {
        return attributeIndex.containsKey(attributeName.toLowerCase());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((attributeIndex == null) ? 0 : attributeIndex.hashCode());
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
        if (attributeIndex == null) {
            if (other.attributeIndex != null)
                return false;
        } else if (!attributeIndex.equals(other.attributeIndex))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "Schema[" + this.attributes.toString() + "]";
    }
    
    public static void checkAttributeNotExists(Schema schema, String attributeName) {
        checkNotNull(schema);
        checkNotNull(attributeName);
        
        if (schema.containsAttribute(attributeName)) {
            throw new TexeraException(ErrorMessages.DUPLICATE_ATTRIBUTE(
                    schema, attributeName));
        }
    }
    
    public static void checkAttributeNotExists(Schema schema, String... attributeNames) {
        checkNotNull(schema);
        checkNotNull(attributeNames);
        
        checkAttributeNotExists(schema, Arrays.asList(attributeNames));
    }
    
    public static void checkAttributeNotExists(Schema schema, Iterable<String> attributeNames) {
        checkNotNull(schema);
        checkNotNull(attributeNames);
        attributeNames.forEach(attrName -> checkNotNull(attrName));
        
        attributeNames.forEach(attrName -> checkAttributeNotExists(schema, attrName));
    }
    
    public static void checkAttributeExists(Schema schema, String attributeName) {
        checkNotNull(schema);
        checkNotNull(attributeName);
        
        if (! schema.containsAttribute(attributeName)) {
            throw new TexeraException(ErrorMessages.ATTRIBUTE_NOT_EXISTS(
                    schema, attributeName));
        }
    }
    
    public static void checkAttributeExists(Schema schema, String... attributeNames) {
        checkNotNull(schema);
        checkNotNull(attributeNames);
        
        checkAttributeExists(schema, Arrays.asList(attributeNames));
    }
    
    public static void checkAttributeExists(Schema schema, Iterable<String> attributeNames) {
        checkNotNull(schema);
        checkNotNull(attributeNames);
        attributeNames.forEach(attrName -> checkNotNull(attrName));
        
        attributeNames.forEach(attrName -> checkAttributeExists(schema, attrName));
    }
    
    
    public static class Builder {
        
        private final ArrayList<Attribute> attributeList;
        private final LinkedHashSet<String> attributeNames;
        
        /**
         * Creates a new builder.
         */
        public Builder() {
            this.attributeList = new ArrayList<>();
            this.attributeNames = new LinkedHashSet<>();
        }
        
        /**
         * Creates a new builder with an existing schema.
         * @param schema
         */
        public Builder(Schema schema) {
            checkNotNull(schema);
            this.attributeList = new ArrayList<>(schema.getAttributes());
            this.attributeNames = new LinkedHashSet<>(schema.getAttributes().stream()
                    .map(attr -> attr.getName().toLowerCase())
                    .collect(Collectors.toSet()));
        }
        
        /**
         * Returns a newly created schema based on the builder.
         */
        public Schema build() {
            return new Schema(this.attributeList);
        }
        
        /********************
         * builder helper functions related to adding attributes to the builder
         ********************/
        
        /**
         * Adds an attribute to the builder.
         * Fails if an attribute with same name (case insensitive) already exists.
         * 
         * @param attribute
         * @return this Builder object
         * @throws TexeraException if the an attribute with the same name already exists
         */
        public Builder add(Attribute attribute) throws TexeraException {
            checkNotNull(attribute);
            checkAttributeNotExists(attribute.getName());
            
            this.attributeList.add(attribute);
            this.attributeNames.add(attribute.getName().toLowerCase());
            return this;
        }
        
        /**
         * Adds an attribute to the builder.
         * Fails if an attribute with same name (case insensitive) already exists.
         * 
         * @param attributeName
         * @param attributeType
         * @return this Builder object
         * @throws TexeraException if the an attribute with the same name already exists
         */
        public Builder add(String attributeName, AttributeType attributeType) throws TexeraException {
            checkNotNull(attributeName);
            checkNotNull(attributeType);
            
            this.add(new Attribute(attributeName, attributeType));
            return this;
        }
        
        /**
         * Adds the attributes to the builder.
         * Fails if an attribute with the same name (case insensitive) already exists.
         * 
         * @param attributes
         * @return this Builder object
         * @throws TexeraException if the an attribute with the same name already exists
         */
        public Builder add(Iterable<Attribute> attributes) throws TexeraException {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkAttributeNotExists(attr.getName()));
            
            attributes.forEach(attr -> this.add(attr));
            return this;
        }
        
        /**
         * Adds the attributes to the builder.
         * Fails if an attribute with the same name (case insensitive) already exists.
         * 
         * @param attributes
         * @return this Builder object
         * @throws TexeraException if the an attribute with the same name already exists
         */
        public Builder add(Attribute... attributes) {
            checkNotNull(attributes);
            
            this.add(Arrays.asList(attributes));
            return this;
        }
        
        /**
         * Adds all the attributes from an existing schema to the builder.
         * Fails if an attribute with the same name (case insensitive) already exists.
         * 
         * @param schema
         * @return this Builder object
         * @throws TexeraException if the an attribute with the same name already exists
         */
        public Builder add(Schema schema) {
            checkNotNull(schema);
            
            this.add(schema.getAttributes());
            return this;
        }
        
        
        /********************
         * builder helper functions related to removing attributes to the builder
         ********************/
        
        /**
         * Removes an attribute from the schema builder if it exists.
         * 
         * @param attribute, the name of the attribute
         * @return this Builder object
         */
        public Builder removeIfExists(String attribute) {
            checkNotNull(attribute);
            
            attributeList.removeIf((Attribute attr) -> attr.getName().equalsIgnoreCase(attribute));
            attributeNames.remove(attribute.toLowerCase());
            return this;
        }
        
        /**
         * Removes the attributes from the schema builder if they exist.
         * 
         * @param attributes, the names of the attributes
         * @return this Builder object
         */
        public Builder removeIfExists(Iterable<String> attributes) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            attributes.forEach(attr -> this.removeIfExists(attr));
            return this;
        }
        
        /**
         * Removes the attributes from the schema builder if they exist.
         * 
         * @param attributes, the names of the attributes
         * @return this Builder object
         */
        public Builder removeIfExists(String... attributes) {
            checkNotNull(attributes);
            
            this.removeIfExists(Arrays.asList(attributes));
            return this;
        }
        
        /**
         * Removes an attribute from the schema builder. 
         * Fails if the attribute does not exist.
         * 
         * @param attribute, the name of the attribute
         * @return this Builder object
         * @throws TexeraException if an attribute does not exist
         */
        public Builder remove(String attribute) throws TexeraException {
            checkNotNull(attribute);
            checkAttributeExists(attribute);
            
            removeIfExists(attribute);
            return this;
        }
        
        /**
         * Removes the attributes from the schema builder. 
         * Fails if an attributes does not exist.
         * 
         * @param attribute
         * @return this Builder object
         * @throws TexeraException if an attribute does not exist
         */
        public Builder remove(Iterable<String> attributes) throws TexeraException {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            attributes.forEach(attr -> checkAttributeExists(attr));
            
            this.removeIfExists(attributes);
            return this;
        }
        
        /**
         * Removes the attributes from the schema builder. 
         * Fails if an attributes does not exist.
         * 
         * @param attribute
         * @return the builder itself
         * @throws TexeraException if an attribute does not exist
         */
        public Builder remove(String... attributes) throws TexeraException {
            checkNotNull(attributes);
            
            this.remove(Arrays.asList(attributes));
            return this;
        }
        
        /********************
         * commonly used public helper functions
         ********************/
        
        /**
         * Creates a new schema, with "_ID" attribute added to the front if it doesn't exist.
         * 
         * @param schema
         * @return
         */
        public static Schema getSchemaWithID(Schema schema) {
            if (schema.containsAttribute(SchemaConstants._ID_ATTRIBUTE.getName())) {
                return schema;
            }
            return new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE).add(schema).build();    
        }
        
        
        /********************
         * private helper functions related to adding/removing attributes to the builder
         ********************/
        
        private void checkAttributeNotExists(String attributeName) {
            if (this.attributeNames.contains(attributeName.toLowerCase())) {
                throw new TexeraException(ErrorMessages.DUPLICATE_ATTRIBUTE(
                        this.attributeNames, attributeName));
            }
        }
        
        private void checkAttributeExists(String attributeName) {
            if (! this.attributeNames.contains(attributeName.toLowerCase())) {
                throw new TexeraException(ErrorMessages.ATTRIBUTE_NOT_EXISTS(
                        this.attributeNames, attributeName));
            }
        }
        
    }

}
