package edu.uci.ics.texera.workflow.common.tuple.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A schema is a list of attributes that describe all the columns of a table.
 */
public class Schema implements Serializable {
    private final List<Attribute> attributes;
    private final Map<String, Integer> attributeIndex;

    public Schema(Attribute... attributes) {
        this(Arrays.asList(attributes));
    }

    @JsonCreator
    public Schema(
            @JsonProperty(value = "attributes", required = true)
                    List<Attribute> attributes) {
        checkNotNull(attributes);
        this.attributes = Collections.unmodifiableList(attributes);
        HashMap<String, Integer> attributeIndexTemp = new HashMap<String, Integer>();
        for (int i = 0; i < attributes.size(); i++) {
            attributeIndexTemp.put(attributes.get(i).getName().toLowerCase(), i);
        }
        this.attributeIndex = Collections.unmodifiableMap(attributeIndexTemp);
    }

    @JsonProperty(value = "attributes")
    public List<Attribute> getAttributes() {
        return attributes;
    }

    public scala.collection.immutable.List<Attribute> getAttributesScala() {
        return JavaConverters.asScalaBuffer(attributes).toList();
    }

    public scala.collection.immutable.List<String> getAttributeNamesScala() {
        return JavaConverters.asScalaBuffer(getAttributeNames()).toList();
    }

    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getName()).collect(Collectors.toList());
    }
    public List<String> getAttributeTypes() {
        return attributes.stream().map(attr -> attr.getType()+"").collect(Collectors.toList());
    }

    public Integer getIndex(String attributeName) {
        if (!containsAttribute(attributeName)) {
            throw new RuntimeException(attributeName + " is not contained in the schema");
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
            throw new RuntimeException(DUPLICATE_ATTRIBUTE(
                    schema.getAttributeNames(), attributeName));
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

        if (!schema.containsAttribute(attributeName)) {
            throw new RuntimeException(ATTRIBUTE_NOT_EXISTS(
                    schema.getAttributeNames(), attributeName));
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

    public static final String DUPLICATE_ATTRIBUTE(Iterable<String> attributeNameList, String attributeName) {
        return String.format("attribute %s already exists in the Schema: %s",
                attributeName, attributeNameList);
    }

    public static final String ATTRIBUTE_NOT_EXISTS(Iterable<String> attributeNameList, String attributeName) {
        return String.format("attribute %s does not exist in the schema: %s",
                attributeName, attributeNameList);
    }

    public static Schema.Builder newBuilder() {
        return new Schema.Builder();
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
         *
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
         */
        public Builder add(Attribute attribute) {
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
         */
        public Builder add(String attributeName, AttributeType attributeType) {
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
         */
        public Builder add(Iterable<Attribute> attributes) {
            checkNotNull(attributes);
            attributes.forEach(attr -> checkAttributeNotExists(attr.getName()));

            attributes.forEach(this::add);
            return this;
        }

        /**
         * Adds the attributes to the builder.
         * Fails if an attribute with the same name (case insensitive) already exists.
         *
         * @param attributes
         * @return this Builder object
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
         */
        public Builder remove(String attribute) {
            checkNotNull(attribute);
            checkAttributeExists(attribute);

            removeIfExists(attribute);
            return this;
        }

        /**
         * Removes the attributes from the schema builder.
         * Fails if an attributes does not exist.
         */
        public Builder remove(Iterable<String> attributes) {
            checkNotNull(attributes);
            attributes.forEach(Preconditions::checkNotNull);
            attributes.forEach(this::checkAttributeExists);

            this.removeIfExists(attributes);
            return this;
        }

        /**
         * Removes the attributes from the schema builder.
         * Fails if an attributes does not exist.
         *
         * @param attributes
         * @return the builder itself
         */
        public Builder remove(String... attributes) {
            checkNotNull(attributes);

            this.remove(Arrays.asList(attributes));
            return this;
        }

        /********************
         * commonly used public helper functions
         ********************/


        /********************
         * private helper functions related to adding/removing attributes to the builder
         ********************/

        private void checkAttributeNotExists(String attributeName) {
            if (this.attributeNames.contains(attributeName.toLowerCase())) {
                throw new RuntimeException(DUPLICATE_ATTRIBUTE(
                        this.attributeNames, attributeName));
            }
        }

        private void checkAttributeExists(String attributeName) {
            if (!this.attributeNames.contains(attributeName.toLowerCase())) {
                throw new RuntimeException(ATTRIBUTE_NOT_EXISTS(
                        this.attributeNames, attributeName));
            }
        }

    }

}
