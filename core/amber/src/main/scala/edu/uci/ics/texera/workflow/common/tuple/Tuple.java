package edu.uci.ics.texera.workflow.common.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.texera.workflow.common.Utils;
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class Tuple implements ITuple, Serializable {

    private final Schema schema;
    private final List<Object> fields;

    public Tuple(Schema schema, Object... fields) {
        this(schema, Arrays.asList(fields));
    }

    @JsonCreator
    public Tuple(
            @JsonProperty(value = "schema", required = true)
                    Schema schema,
            @JsonProperty(value = "fields", required = true)
                    List<Object> fields) {
        // check arguments are not null
        checkNotNull(schema);
        checkNotNull(fields);

        // check schema matches the fields
        checkSchemaMatchesFields(schema.getAttributes(), fields);

        this.schema = schema;
        this.fields = Collections.unmodifiableList(fields);
    }

    @Override
    @JsonIgnore
    public int length() {
        return fields.size();
    }

    @Override
    @JsonIgnore
    public Object get(int i) {
        return fields.get(i);
    }

    @Override
    @JsonIgnore
    public Object[] toArray() {
        Object[] array = new Object[0];
        return fields.toArray(array);
    }

    @JsonProperty(value = "schema")
    public Schema getSchema() {
        return schema;
    }

    @JsonProperty(value = "fields")
    public List<Object> getFields() {
        return this.fields;
    }

    @SuppressWarnings("unchecked")
    public <T> T getField(String attributeName) {
        if (!schema.containsAttribute(attributeName)) {
            throw new RuntimeException(attributeName + " is not in the tuple");
        }
        return (T) fields.get(schema.getIndex(attributeName));
    }

    public <T> T getField(String attributeName, Class<T> fieldClass) {
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
            return other.schema == null;
        } else {
            return schema.equals(other.schema);
        }
    }

    public String toString() {
        return "Tuple [schema=" + schema + ", fields=" + fields + "]";
    }

    /**
     * Get a human-readable json of this tuple.
     * The format is :
     * {
     * "attr1": "field1",
     * "attr2": "field2"
     * }
     * <p>
     * The difference of this json version and default json version is that
     * it no longer keeps the type of the attribute.
     * Therefore it cannot be converted back to a tuple object again.
     *
     * @return
     */
    public ObjectNode asKeyValuePairJson() {
        ObjectNode objectNode = Utils.objectMapper().createObjectNode();
        for (String attrName : this.schema.getAttributeNames()) {
            JsonNode valueNode = Utils.objectMapper().convertValue(this.getField(attrName), JsonNode.class);
            objectNode.set(attrName, valueNode);
        }
        return objectNode;
    }

    /*
     * Checks if the list of attributes matches the list of fields
     */
    private static void checkSchemaMatchesFields(Iterable<Attribute> attributes, Iterable<Object> fields) {
        List<Attribute> attributeList = Lists.newArrayList(attributes);
        List<Object> fieldList = Lists.newArrayList(fields);

        // check schema's size and field's size are the same
        if (attributeList.size() != fieldList.size()) {
            throw new RuntimeException(String.format(
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
    private static void checkAttributeMatchesField(Attribute attribute, Object field) {
        // null value is always acceptable
        if (field == null) {
            return;
        }
        // ANY attribute type allow arbitrary type
        if (attribute.getType() == AttributeType.ANY) {
            return;
        }
        if (!field.getClass().equals(attribute.getType().getFieldClass())) {
            throw new RuntimeException(String.format(
                    "Attribute %s's type (%s) is different from field's type (%s)",
                    attribute.getName(), attribute.getType(),
                    AttributeType.getAttributeType(field.getClass())));
        }
    }

    public static Tuple.Builder newBuilder() {
        return new Tuple.Builder();
    }

    public static Tuple.BuilderV2 newBuilder(Schema schema) {
        return new Tuple.BuilderV2(schema);
    }

    /**
     * Tuple.Builder is a helper class for creating immutable Tuple instances.
     * <p>
     * Since Tuple is immutable, Tuple.Builder provides a set of commonly used functions
     * to do insert/remove operations.
     * <p>
     * Tuple.Builder also provides a set of static helper function to manipulate a list of tuples.
     *
     * @author Zuozhi Wang
     */
    public static class Builder {

        private final Schema.Builder schemaBuilder;
        private final HashMap<String, Object> fieldNameMap;

        /**
         * Creates a new Tuple Builder.
         */
        public Builder() {
            this.schemaBuilder = new Schema.Builder();
            this.fieldNameMap = new HashMap<>();
        }

        /**
         * Creates a new Tuple Builder based on an existing tuple object.
         *
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
         *
         * @return
         */
        public Tuple build() {
            Schema schema = schemaBuilder.build();
            ArrayList<Object> fields = new ArrayList<>();
            for (int i = 0; i < schema.getAttributes().size(); i++) {
                fields.add(fieldNameMap.get(schema.getAttributes().get(i).getName().toLowerCase()));
            }
            return new Tuple(schema, fields);
        }

        /**
         * Adds an existing tuple to the tuple builder.
         */
        public Builder add(Tuple tuple) {
            return add(tuple.schema.getAttributes(), tuple.fields);
        }

        /**
         * Adds a new attribute and field to the tuple builder.
         *
         * @param attribute
         * @param field
         * @return this builder object
         * @throws RuntimeException, if attribute already exists, or the attribute and field type don't match.
         */
        public Builder add(Attribute attribute, Object field) {
            checkNotNull(attribute);
            checkAttributeMatchesField(attribute, field);

            // schema builder will check if the attribute already exists
            schemaBuilder.add(attribute);
            fieldNameMap.put(attribute.getName().toLowerCase(), field);
            return this;
        }

        /**
         * Adds a new attribute and field to the tuple builder
         *
         * @param attributeName
         * @param attributeType
         * @param field
         * @return this builder object
         * @throws RuntimeException, if attribute already exists, or the attribute and field type don't match.
         */
        public Builder add(String attributeName, AttributeType attributeType, Object field) throws RuntimeException {
            checkNotNull(attributeName);
            checkNotNull(attributeType);

            this.add(new Attribute(attributeName, attributeType), field);
            return this;
        }

        public Builder add(Schema schema, Iterable<Object> fields) {
            return add(schema.getAttributes(), fields);
        }

        public Builder add(Schema schema, Object[] fields) {
            return add(schema.getAttributes(), Lists.newArrayList(fields));
        }

        /**
         * Adds a list of new attributes and fields to the tuple builder.
         * Each attribute in the list corresponds to the field with the same index.
         *
         * @param attributes
         * @param fields
         * @return this builder object
         * @throws RuntimeException if one of the attributes already exists, or attributes and fields don't match
         */
        public Builder add(Iterable<Attribute> attributes, Iterable<Object> fields) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            checkNotNull(fields);

            checkSchemaMatchesFields(attributes, fields);

            int attrSize = Iterables.size(attributes);
            Iterator<Attribute> attributesIterator = attributes.iterator();
            Iterator<Object> fieldsIterator = fields.iterator();

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
         * @throws RuntimeException, if the attribute doesn't exist
         */
        public Builder remove(String attribute) throws RuntimeException {
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
         * @throws RuntimeException, if one of the attributes doesn't exist
         */
        public Builder remove(Iterable<String> attributes) throws RuntimeException {
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
         * @throws RuntimeException, if one of the attributes doesn't exist
         */
        public Builder remove(String... attributes) throws RuntimeException {
            checkNotNull(attributes);

            remove(Arrays.asList(attributes));
            return this;
        }

        /**
         * Removes an attribute (and its corresponding field) from the tuple builder
         * if the attribute exists.
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
         * if the attributes exist.
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
         * @throws RuntimeException, if the attribute already exists, or the attribute and field don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, Attribute attribute, Object field) throws RuntimeException {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attribute);

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
         * @throws RuntimeException, if the attribute already exists, or the attribute and field don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, String attributeName, AttributeType attributeType, Object field) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributeName);
            checkNotNull(attributeType);

            return add(tuples, new Attribute(attributeName, attributeType), field);
        }

        /**
         * Adds a list of new attributes and fields to each tuple in the list.
         *
         * @param tuples
         * @param attributes
         * @param fields
         * @return a list of newly created tuples, with the attributes and fields added.
         * @throws RuntimeException, if one of the attributes already exists, or the attributes and fields don't match
         */
        public static List<Tuple> add(Iterable<Tuple> tuples, Iterable<Attribute> attributes, Iterable<Object> fields) {
            checkNotNull(tuples);
            tuples.forEach(tuple -> checkNotNull(tuple));
            checkNotNull(attributes);
            attributes.forEach(attr -> checkNotNull(attr));
            checkNotNull(fields);

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
         * @throws RuntimeException, if the attribute doesn't exist.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, String attribute) throws RuntimeException {
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
         *
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed
         * @throws RuntimeException, if one of the attributes doesn't exist in the tuples.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, Iterable<String> attributes) {
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
         *
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed
         * @throws RuntimeException, if one of the attributes doesn't exist in the tuples.
         */
        public static List<Tuple> remove(Iterable<Tuple> tuples, String... attributes) {
            return (remove(tuples, Arrays.asList(attributes)));
        }

        /**
         * Removes an attributes (and its corresponding field) from each tuple in the list.
         * if the attribute exists
         *
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
         * if the attributes exist
         *
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
         * if the attributes exist
         *
         * @param tuples
         * @param attributes
         * @return a list of newly created tuples, with the attributes removed (if they exist)
         */
        public static List<Tuple> removeIfExists(Iterable<Tuple> tuples, String... attributes) {
            return (removeIfExists(tuples, Arrays.asList(attributes)));
        }

    }

    public static class BuilderV2 {
        private final Schema schema;
        private final Map<String, Object> fieldNameMap;

        public BuilderV2(Schema schema) {
            this.schema = schema;
            this.fieldNameMap = new HashMap<>();
        }

        /**
         * The tuple argument here is expected to conform to the exact same schema as the
         * the schema passed in the constructor. If it doesn't conform, an error will be thrown.
         * If you wish to ignore attributes in the tuple that are not part of the output schema,
         * then use {@link Tuple.BuilderV2#add(Tuple, boolean)} with second parameter as false
         */
        public BuilderV2 add(Tuple tuple) {
            return add(tuple, true);
        }

        public BuilderV2 add(Tuple tuple, boolean isStrictSchemaMatch) {
            checkNotNull(tuple);

            for (int i = 0; i < tuple.size(); i++) {
                Attribute attribute = tuple.getSchema().getAttributes().get(i);
                // The isStrictSchemaMatch parameter toggles the ability to check exact schema matching.
                // This is so that we don't need a "remove" ever. So, if a tuple is passed in and has more fields
                // than the required schema, we'll assume that the output tuple doesn't need those attributes,
                // PROVIDED isStrictSchemaMatch=false
                if (!isStrictSchemaMatch && !schema.containsAttribute(attribute.getName())) {
                    continue;
                }
                add(attribute, tuple.getFields().get(i));
            }

            return this;
        }

        public BuilderV2 add(Attribute attribute, Object field) {
            checkNotNull(attribute);
            checkAttributeMatchesField(attribute, field);

            if (!schema.containsAttribute(attribute.getName())) {
                throw new TupleBuildingException(String.format("%s doesn't exist in the expected schema.", attribute.getName()));
            }

            fieldNameMap.put(attribute.getName().toLowerCase(), field);
            return this;
        }

        public BuilderV2 add(String attributeName, AttributeType attributeType, Object field) {
            checkNotNull(attributeName);
            checkNotNull(attributeType);

            this.add(new Attribute(attributeName, attributeType), field);
            return this;
        }

        public BuilderV2 addSequentially(Object[] fields) {
            checkNotNull(fields);
            checkSchemaMatchesFields(schema.getAttributes(), Lists.newArrayList(fields));

            for (int i = 0; i < fields.length; i++) {
                fieldNameMap.put(schema.getAttributes().get(i).getName(), fields[i]);
            }

            return this;
        }

        public Tuple build() {
            if (schema.getAttributes().size() != fieldNameMap.size()) {
                throw new TupleBuildingException(
                        String.format("Tuple does not have same number of fields as schema. Has %d, required %d.",
                                fieldNameMap.size(), schema.getAttributes().size())
                );
            }

            List<Object> fields = schema.getAttributes().stream()
                    .map(attribute -> fieldNameMap.get(attribute.getName().toLowerCase()))
                    .collect(Collectors.toList());
            return new Tuple(schema, fields);
        }
    }
}
