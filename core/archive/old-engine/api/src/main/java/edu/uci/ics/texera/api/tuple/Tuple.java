package edu.uci.ics.texera.api.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

/**
 * A Tuple is a set of values(fields) with their names as defined in the schema.
 * A Tuple can be considered as a record/a row in a table.
 * 
 * Tuple instances are immutable. Use Tuple.Builder to create/manipulate Tuple objects.
 * 
 * @author chenli
 * @author sandeepreddy602
 * @author Zuozhi Wang
 * 
 * Created on 3/25/16.
 */
@JsonDeserialize(using = TupleJsonDeserializer.class)
public class Tuple {
    private final Schema schema;
    private final ImmutableList<IField> fields;

    public Tuple(Schema schema, IField... fields) {
        this(schema, Arrays.asList(fields));
    }
    
    @JsonCreator
    public Tuple(
            @JsonProperty(value = JsonConstants.SCHEMA, required = true)
            Schema schema, 
            @JsonProperty(value = JsonConstants.FIELDS, required = true)
            List<IField> fields) {
        // check arguments are not null
        checkNotNull(schema);
        checkNotNull(fields);
        fields.forEach(field -> checkNotNull(field));
        
        // check schema matches the fields
        checkSchemaMatchesFields(schema.getAttributes(), fields);

        this.schema = schema;
        this.fields = ImmutableList.copyOf(fields);
        if (this.fields == null) {
            throw new TexeraException("something goes wrong here");
        }
    }
    
    @JsonProperty(value = JsonConstants.SCHEMA)
    public Schema getSchema() {
        return schema;
    }
    
    @JsonProperty(value = JsonConstants.FIELDS)
    public List<IField> getFields() {
        return this.fields;
    }

    @SuppressWarnings("unchecked")
    public <T extends IField> T getField(String attributeName) {
        if (! schema.containsAttribute(attributeName)) {
            throw new TexeraException(attributeName + " is not in the tuple");
        }
        return (T) fields.get(schema.getIndex(attributeName));
    }
    
    public <T extends IField> T getField(String attributeName, Class<T> fieldClass) {
        return getField(attributeName);
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        return true;
    }

    public String toString() {
        return "Tuple [schema=" + schema + ", fields=" + fields + "]";
    }
    
    /**
     * Get a human-readable json of this tuple. 
     * The format is :
     * {
     *   "attr1": "field1",
     *   "attr2": "field2"
     * }
     * 
     * The difference of this json version and default json version is that
     *   it no longer keeps the type of the attribute.
     * Therefore it cannot be converted back to a tuple object again.
     * @return
     */
    public ObjectNode getReadableJson() {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        for (String attrName : this.schema.getAttributeNames()) {
            JsonNode valueNode = new ObjectMapper().convertValue(this.getField(attrName), JsonNode.class).get(JsonConstants.FIELD_VALUE);
            objectNode.set(attrName, valueNode);
        }
        return objectNode;
    }
    
    /*
     * Checks if the list of attributes matches the list of fields
     */
    private static void checkSchemaMatchesFields(Iterable<Attribute> attributes, Iterable<IField> fields) throws TexeraException {
        List<Attribute> attributeList = Lists.newArrayList(attributes);
        List<IField> fieldList = Lists.newArrayList(fields);
        
        // check schema's size and field's size are the same
        if (attributeList.size() != fieldList.size()) {
            throw new TexeraException(String.format(
                    "Schema size (%d) and field size (%d) are different", 
                    attributeList.size(), fieldList.size()));
        }
        
        // check schema's type and field's type match
        for (int i = 0; i < fieldList.size(); i++) {
            checkAttributeMatchesField(attributeList.get(i), fieldList.get(i));
        }
    }
    
    /*
     * Checks if the attribute's type matches the field object's type
     */
    private static void checkAttributeMatchesField(Attribute attribute, IField field) throws TexeraException {
        if (! field.getClass().equals(attribute.getType().getFieldClass())) {
            throw new TexeraException(String.format(
                    "Attribute %s's type (%s) is different from field's type (%s)", 
                    attribute.getName(), attribute.getType(),
                    AttributeType.getAttributeType(field.getClass())));
        }
    }
    
    /**
     * Tuple.Builder is a helper class for creating immutable Tuple instances.
     * 
     * Since Tuple is immutable, Tuple.Builder provides a set of commonly used functions
     *   to do insert/remove operations.
     * 
     * Tuple.Builder also provides a set of static helper function to manipulate a list of tuples.
     * 
     * @author Zuozhi Wang
     *
     */
    public static class Builder {
        
        private final Schema.Builder schemaBuilder;
        private final HashMap<String, IField> fieldNameMap;
        
        /**
         * Creates a new Tuple Builder.
         */
        public Builder() {
            this.schemaBuilder = new Schema.Builder();
            this.fieldNameMap = new HashMap<>();
        }
        
        /**
         * Creates a new Tuple Builder based on an existing tuple object.
         * @param tuple
         */
        public Builder(Tuple tuple) {
            checkNotNull(tuple);
            checkNotNull(tuple.getFields());
            
            this.schemaBuilder = new Schema.Builder(tuple.getSchema());
            this.fieldNameMap = new HashMap<>();
            for (int i = 0; i < tuple.getFields().size(); i++) {
                this.fieldNameMap.put(
                        tuple.getSchema().getAttributes().get(i).getName().toLowerCase(), 
                        tuple.fields.get(i));
            }
        }
        
        /**
         * Builds a newly created Tuple based on the builder.
         * @return
         */
        public Tuple build() {
            Schema schema = schemaBuilder.build();
            ArrayList<IField> fields = new ArrayList<>();
            for (int i = 0; i < schema.getAttributes().size(); i++) {
                fields.add(fieldNameMap.get(schema.getAttributes().get(i).getName().toLowerCase()));
            }
            return new Tuple(schema, fields);
        }
        
        /**
         * Adds a new attribute and field to the tuple builder.
         * 
         * @param attribute
         * @param field
         * @return this builder object
         * @throws TexeraException, if attribute already exists, or the attribute and field type don't match.
         */
        public Builder add(Attribute attribute, IField field) throws TexeraException {
            checkNotNull(attribute);
            checkNotNull(field);
            checkAttributeMatchesField(attribute, field);
            
            // schema builder will check if the attribute already exists
            schemaBuilder.add(attribute);
            fieldNameMap.put(attribute.getName().toLowerCase(), field);
            return this;
        }
        
        /**
         * Adds a new attribute and field to the tuple builder
         * @param attributeName
         * @param attributeType
         * @param field
         * @return this builder object
         * @throws TexeraException, if attribute already exists, or the attribute and field type don't match.
         */
        public Builder add(String attributeName, AttributeType attributeType, IField field) throws TexeraException {
            checkNotNull(attributeName);
            checkNotNull(attributeType);
            checkNotNull(field);
            
            this.add(new Attribute(attributeName, attributeType), field);
            return this;
        }
        
        /**
         * Adds a list of new attributes and fields to the tuple builder.
         * Each attribute in the list corresponds to the field with the same index.
         * 
         * @param attributes
         * @param fields
         * @return this builder object
         * @throws TexeraException if one of the attributes already exists, or attributes and fields don't match
         */
        public Builder add(Iterable<Attribute> attributes, Iterable<IField> fields) throws TexeraException {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            checkNotNull(fields);
            fields.forEach(field -> checkNotNull(field));
            
            checkSchemaMatchesFields(attributes, fields);
            
            int attrSize = Iterables.size(attributes);
            Iterator<Attribute> attributesIterator = attributes.iterator();
            Iterator<IField> fieldsIterator = fields.iterator();
            
            for (int i = 0; i < attrSize; i++) {
                this.add(attributesIterator.next(), fieldsIterator.next());
            }
            return this;
        }
        
        /**
         * Removes an attribute (and its corresponding field) from the tuple builder.
         * 
         * @param attribute, the name of the attribute
         * @return this builder object
         * @throws TexeraException, if the attribute doesn't exist
         */
        public Builder remove(String attribute) throws TexeraException {
            checkNotNull(attribute);
            
            // schemaBuilder will check if the attribute exists
            schemaBuilder.remove(attribute);
            fieldNameMap.remove(attribute.toLowerCase());  
            return this;
        }
        
        /**
         * Removes a list of attributes (and their corresponding fields) from the tuple builder.
         * 
         * @param attributes, the names of the attributes
         * @return this builder object
         * @throws TexeraException, if one of the attributes doesn't exist
         */
        public Builder remove(Iterable<String> attributes) throws TexeraException {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            attributes.forEach(attr -> this.remove(attr));
            return this;
        }
        
        /**
         * Removes the attributes (and their corresponding fields) from the tuple builder.
         * 
         * @param attributes, the names of the attributes
         * @return this builder object
         * @throws TexeraException, if one of the attributes doesn't exist
         */
        public Builder remove(String... attributes) throws TexeraException{
            checkNotNull(attributes);
            
            remove(Arrays.asList(attributes));
            return this;
        }
        
        /**
         * Removes an attribute (and its corresponding field) from the tuple builder 
         *   if the attribute exists.
         * 
         * @param attribute, the name of the attribute
         * @return this builder object
         */
        public Builder removeIfExists(String attribute) {
            checkNotNull(attribute);
            
            schemaBuilder.removeIfExists(attribute);
            fieldNameMap.remove(attribute.toLowerCase());
            return this;
        }
        
        /**
         * Removes a list of attributes (and their corresponding field) from the tuple builder 
         *   if the attributes exist.
         * 
         * @param attributes, the names of the attributes
         * @return this builder object
         */
        public Builder removeIfExists(Iterable<String> attributes) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            attributes.forEach(attr -> this.removeIfExists(attr));
            return this;
        }
        
        /**
         * Removes a list of attributes (and their corresponding field) from the tuple builder if the attributes exist.
         * 
         * @param attributes, the names of the attributes
         * @return this builder object
         */
        public Builder removeIfExists(String... attributes) {
            checkNotNull(attributes);
            
            removeIfExists(Arrays.asList(attributes));
            return this;
        }
        
        /*********************
         * public static helper functions to handle a list of tuples
         *********************/
        
        /**
         * Adds a new attribute and field to each tuple in the list.
         * 
         * @param tuples
         * @param attribute
         * @param field
         * @return a list of newly created tuples, with the attribute and field added.
         * @throws TexeraException, if the attribute already exists, or the attribute and field don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, Attribute attribute, IField field) throws TexeraException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);
            checkNotNull(field);
            
            return Streams.stream(tuples)
                .map(tuple -> new Tuple.Builder(tuple))
                .map(builder -> builder.add(attribute, field))
                .map(builder -> builder.build())
                .collect(Collectors.toList());
        }
        
        /**
         * Adds a new attribute and field to each tuple in the list.
         * 
         * @param tuples
         * @param attributeName
         * @param attributeType
         * @param field
         * @return a list of newly created tuples, with the attribute and field added.
         * @throws TexeraException, if the attribute already exists, or the attribute and field don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, String attributeName, AttributeType attributeType, IField field) throws TexeraException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributeName);
            checkNotNull(attributeType);
            checkNotNull(field);
            
            return add(tuples, new Attribute(attributeName, attributeType), field);
        }
    
        /**
         * Adds a list of new attributes and fields to each tuple in the list.
         * 
         * @param tuples
         * @param attributes
         * @param fields
         * @return a list of newly created tuples, with the attributes and fields added.
         * @throws TexeraException, if one of the attributes already exists, or the attributes and fields don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, Iterable<Attribute> attributes, Iterable<IField> fields) throws TexeraException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            checkNotNull(fields);
            fields.forEach(field -> checkNotNull(field));
            
            return Streams.stream(tuples)
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.add(attributes, fields))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());            
        }
        
        /**
         * Removes an attribute (and its corresponding field) from each tuple in the list.
         * 
         * @param tuples
         * @param attribute
         * @return a list of newly created tuples, with the attribute removed.
         * @throws TexeraException, if the attribute doesn't exist.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, String attribute) throws TexeraException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);
            
            return Streams.stream(tuples)
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.remove(attribute))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        /**
         * Removes a list of attributes (and their corresponding fields) from each tuple in the list.
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed
         * @throws TexeraException, if one of the attributes doesn't exist in the tuples.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, Iterable<String> attributes) throws TexeraException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            return Streams.stream(tuples)
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.remove(attributes))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        /**
         * Removes a list of attributes (and their corresponding fields) from each tuple in the list.
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed
         * @throws TexeraException, if one of the attributes doesn't exist in the tuples.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, String... attributes) throws TexeraException {
            return(remove(tuples, Arrays.asList(attributes)));
        }
        
        /**
         * Removes an attributes (and its corresponding field) from each tuple in the list.
         *   if the attribute exists
         * @param tuples
         * @param attribute
         * @return a list of newly created tuples, with the attributes removed (if they exist)
         */
        public static List<Tuple> removeIfExists(Iterable<Tuple> tuples, String attribute) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);
            
            return Streams.stream(tuples)
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.removeIfExists(attribute))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }

        /**
         * Removes a list of attributes (and their corresponding fields) from each tuple in the list.
         *   if the attributes exist
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed (if they exist)
         */
        public static List<Tuple> removeIfExists(Iterable<Tuple> tuples, Iterable<String> attributes) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            
            return Streams.stream(tuples)
                    .map(tuple -> new Tuple.Builder(tuple))
                    .map(builder -> builder.removeIfExists(attributes))
                    .map(builder -> builder.build())
                    .collect(Collectors.toList());
        }
        
        /**
         * Removes a list of attributes (and their corresponding fields) from each tuple in the list 
         *   if the attributes exist
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed (if they exist)
         */
        public static List<Tuple> removeIfExists(Iterable<Tuple> tuples, String... attributes) {
            return(removeIfExists(tuples, Arrays.asList(attributes)));
        }
        
    }
    
}
