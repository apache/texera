package texera.common.schema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import texera.common.TexeraUtils;
import texera.common.workflow.TexeraOperator;

@SuppressWarnings("unchecked")
public class OperatorSchemaGenerator {

    public static final ObjectMapper objectMapper = TexeraUtils.objectMapper();

    // a map of all predicate classes (declared in PredicateBase) and their operatorType string
    public static final HashMap<Class<? extends TexeraOperator>, String> operatorTypeMap = new HashMap<>();
    static {
        // find all the operator type declarations in PredicateBase annotation
        Collection<NamedType> types = objectMapper.getSubtypeResolver().collectAndResolveSubtypesByClass(
                objectMapper.getDeserializationConfig(),
                AnnotatedClass.construct(objectMapper.constructType(TexeraOperator.class),
                        objectMapper.getDeserializationConfig()));

        // populate the operatorType map
        for (NamedType type : types) {
            if (type.getType() != null && type.getName() != null) {
                operatorTypeMap.put((Class<? extends TexeraOperator>) type.getType(), type.getName());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        generateAllOperatorSchema();
//        generateJsonSchema(ComparablePredicate.class);
    }

    public static void generateAllOperatorSchema() throws Exception {
        for (Class<? extends TexeraOperator> predicateClass : operatorTypeMap.keySet()) {
            generateOperatorSchema(predicateClass);
        }
    }

    public static void generateOperatorSchema(Class<? extends TexeraOperator> predicateClass) throws Exception {

        if (! operatorTypeMap.containsKey(predicateClass)) {
            throw new RuntimeException("predicate class " + predicateClass.toString() + " is not registerd in TexeraOperatorDescription class");
        }

        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);

        // find the operator json schema path by its predicate class
        Path operatorSchemaPath = getJsonSchemaPath(predicateClass);

        // create the json schema file of the operator
        Files.deleteIfExists(operatorSchemaPath);
        Files.createFile(operatorSchemaPath);

        // generate the json schema
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(OperatorSchemaGenerator.objectMapper);
        JsonSchema schema = jsonSchemaGenerator.generateSchema(predicateClass);

        ObjectNode schemaNode = objectMapper.readValue(objectMapper.writeValueAsBytes(schema), ObjectNode.class);
        ObjectNode propertiesNode = ((ObjectNode) schemaNode.get("properties"));

        // remove the operatorID from the json schema
        propertiesNode.remove("operatorID");

        // process each property due to frontend form generator requirements
        propertiesNode.fields().forEachRemaining(e -> {
            String propertyName = e.getKey();
            ObjectNode propertyNode = (ObjectNode) e.getValue();

            // add a "title" field to each property
            propertyNode.put("title", propertyName);
            // if property is an enum, set unique items to true
            if (propertiesNode.has("enum")) {
                propertyNode.put("uniqueItems", true);
            }
        });

        // TODO: add required/optional properties to the schema
//        List<String> requiredProperties = getRequiredProperties(predicateClass);
//        // don't add the required properties if it's empty
//        // because draft v4 doesn't allow it
//        if (! requiredProperties.isEmpty()) {
//            schemaNode.set("required", objectMapper.valueToTree(requiredProperties));
//        }

        // TODO: add property default values to the schema
//        Map<String, Object> defaultValues = getPropertyDefaultValues(predicateClass);
//        for (String property : defaultValues.keySet()) {
//            ObjectNode propertyNode = (ObjectNode) schemaNode.get("properties").get(property);
//            propertyNode.set("default", objectMapper.convertValue(defaultValues.get(property), JsonNode.class));
//        }

        // add additional operator metadata to the additionalMetadataNode
        TexeraOperatorDescription texeraOperatorDescription = predicateClass.getConstructor().newInstance().texeraOperatorDescription();
        JsonNode operatorDescriptionNode = objectMapper.valueToTree(texeraOperatorDescription);

        // setup the full metadata node, which contains the schema and additional metadata
        ObjectNode fullMetadataNode = objectMapper.createObjectNode();

        // add the operator type to the full node
        fullMetadataNode.put("operatorType", operatorType);
        fullMetadataNode.set("jsonSchema", schemaNode);
        fullMetadataNode.set("additionalMetadata", operatorDescriptionNode);

        Files.write(operatorSchemaPath, objectMapper.writeValueAsBytes(fullMetadataNode));

        System.out.println("generating schema of " + operatorType + " completed");
        System.out.println(fullMetadataNode);
    }

    public static Path getJsonSchemaPath(Class<? extends TexeraOperator> predicateClass) {
        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);

        // find the operator directory by its predicate class
        Path classDirectory = TexeraUtils.texeraHomePath()
                .resolve("src/main/scala")
                .resolve(predicateClass.getPackage().getName().replace('.', '/'));

        return classDirectory.resolve(operatorType + "Schema.json");
    }

}
