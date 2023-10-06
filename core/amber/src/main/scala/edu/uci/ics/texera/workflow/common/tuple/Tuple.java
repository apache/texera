package edu.uci.ics.texera.workflow.common.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.texera.Utils;
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.bson.Document;
import org.ehcache.sizeof.SizeOf;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class Tuple implements ITuple, Serializable {

    private final Schema schema;
    private final List<Object> fields;

    private final Long inMemSize;

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

        this.inMemSize = SizeOf.newInstance().deepSizeOf(this);
    }

    @Override
    public long inMemSize() {
        return this.inMemSize;
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
        return Arrays.deepHashCode(fields.toArray());
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

    public Tuple getPartialTuple(int[] indices){
        Schema partialSchema = schema.getPartialSchema(indices);
        Tuple.BuilderV2 builder = new Tuple.BuilderV2(partialSchema);
        Object[] partialArray = new Object[indices.length];
        for (int i = 0; i < indices.length; i++) {
            partialArray[i] = fields.get(indices[i]);
        }
        builder.addSequentially(partialArray);
        return builder.build();
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
     * convert the tuple to a bson document for mongoDB storage
     */
    public Document asDocument() {
        Document doc = new Document();
        for (String attrName : this.schema.getAttributeNames()) {
            doc.put(attrName, this.getField(attrName));
        }
        return doc;
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

    public static Tuple.BuilderV2 newBuilder(Schema schema) {
        return new Tuple.BuilderV2(schema);
    }

    /**
     * {@link Tuple.BuilderV2} is a helper class for creating immutable Tuple instances.
     * <p>
     *
     * @author Maaz Syed Adeeb
     */
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

        /**
         * Adds the array of the fields provided using the attributes provided in the
         * schema.
         */
        public BuilderV2 addSequentially(Object[] fields) {
            checkNotNull(fields);
            checkSchemaMatchesFields(schema.getAttributes(), Lists.newArrayList(fields));
            for (int i = 0; i < fields.length; i++) {
                this.add(schema.getAttributes().get(i), fields[i]);
            }
            return this;
        }

        public Tuple build() {
            // Partition the attributes to a list of attributes present and absent in the fieldNameMap.
            // This helps in printing a better error message using the missing attributes
            Map<Boolean, List<Attribute>> partitionedAttributes = schema.getAttributes().stream()
                    .collect(Collectors.partitioningBy(attribute -> fieldNameMap.containsKey(attribute.getName().toLowerCase())));

            List<Attribute> missingAttributes = partitionedAttributes.get(false);
            List<Attribute> availableAttributes = partitionedAttributes.get(true);

            if (!missingAttributes.isEmpty()) {
                throw new TupleBuildingException(
                        String.format("Tuple does not have same number of attributes as schema. Has %d, required %d.%nMissing attributes are %s",
                                fieldNameMap.size(), schema.getAttributes().size(), missingAttributes)
                );
            }

            List<Object> fields = availableAttributes.stream()
                    .map(attribute -> fieldNameMap.get(attribute.getName().toLowerCase()))
                    .collect(Collectors.toList());
            return new Tuple(schema, fields);
        }
    }
}
