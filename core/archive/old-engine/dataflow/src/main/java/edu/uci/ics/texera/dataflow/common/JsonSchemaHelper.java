package edu.uci.ics.texera.dataflow.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.plangen.OperatorArityConstants;

@SuppressWarnings("unchecked")
public class JsonSchemaHelper {
    
    private static ObjectMapper objectMapper = DataConstants.defaultObjectMapper;
    
    // a map of all predicate classes (declared in PredicateBase) and their operatorType string
    public static HashMap<Class<? extends PredicateBase>, String> operatorTypeMap = new HashMap<>();
    static {
        // find all the operator type declarations in PredicateBase annotation
        Collection<NamedType> types = objectMapper.getSubtypeResolver().collectAndResolveSubtypesByClass(
                objectMapper.getDeserializationConfig(), 
                AnnotatedClass.construct(objectMapper.constructType(PredicateBase.class),
                        objectMapper.getDeserializationConfig()));
        
        // populate the operatorType map
        for (NamedType type : types) {
            if (type.getType() != null && type.getName() != null) {
                operatorTypeMap.put((Class<? extends PredicateBase>) type.getType(), type.getName());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        generateAllOperatorSchema();
//        generateJsonSchema(ComparablePredicate.class);
    }
    
    public static void generateAllOperatorSchema() throws Exception {
        for (Class<? extends PredicateBase> predicateClass : operatorTypeMap.keySet()) {
            generateJsonSchema(predicateClass);
        }
    }
    
    public static void generateJsonSchema(Class<? extends PredicateBase> predicateClass) throws Exception {
        
        if (! operatorTypeMap.containsKey(predicateClass)) {
            throw new TexeraException("predicate class " + predicateClass.toString() + " is not registerd in PredicateBase class");
        }

        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);
        
        // find the operator json schema path by its predicate class
        Path operatorSchemaPath = getJsonSchemaPath(predicateClass);
        
        // create the json schema file of the operator
        Files.deleteIfExists(operatorSchemaPath);
        Files.createFile(operatorSchemaPath);   
        
        // generate the json schema
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(DataConstants.defaultObjectMapper);
        JsonSchema schema = jsonSchemaGenerator.generateSchema(predicateClass);

        ObjectNode schemaNode = objectMapper.readValue(objectMapper.writeValueAsBytes(schema), ObjectNode.class);

        LinkedHashSet<JsonNode> propertiesNodes = new LinkedHashSet<>();
        searchForEntity(schemaNode, "properties", propertiesNodes);
        for (JsonNode propertiesJsonNode: propertiesNodes) {
            ObjectNode propertiesNode = (ObjectNode) propertiesJsonNode;
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
        }

        // add required/optional properties to the schema
        List<String> requiredProperties = getRequiredProperties(predicateClass);
        
        // don't add the required properties if it's empty 
        // because draft v4 doesn't allow it
        if (! requiredProperties.isEmpty()) {
            schemaNode.set("required", objectMapper.valueToTree(requiredProperties));
        }
        
        // add property default values to the schema
        Map<String, Object> defaultValues = getPropertyDefaultValues(predicateClass);
        for (String property : defaultValues.keySet()) {
            ObjectNode propertyNode = (ObjectNode) schemaNode.get("properties").get(property);
            propertyNode.set("default", objectMapper.convertValue(defaultValues.get(property), JsonNode.class));
        }
        
        
        // add the additionalMetadataNode
        ObjectNode additionalMetadataNode = objectMapper.createObjectNode();
        
        // add additional operator metadata to the additionalMetadataNode
        Map<String, Object> operatorMetadata = (Map<String, Object>) predicateClass.getMethod("getOperatorMetadata").invoke(null);
        for (String key : operatorMetadata.keySet()) {
            additionalMetadataNode.set(key, objectMapper.valueToTree(operatorMetadata.get(key)));
        }
        
        // add input and output arity to the schema
        additionalMetadataNode.put("numInputPorts", OperatorArityConstants.getFixedInputArity(predicateClass));
        additionalMetadataNode.put("numOutputPorts", OperatorArityConstants.getFixedOutputArity(predicateClass));    
        
        // add advancedOption properties to the schema
        List<String> advancedOptionProperties = getAdvancedOptionProperties(predicateClass);
        additionalMetadataNode.set("advancedOptions", objectMapper.valueToTree(advancedOptionProperties));
        
        
        // setup the full metadata node, which contains the schema and additional metadata
        ObjectNode fullMetadataNode = objectMapper.createObjectNode();
        
        // add the operator type to the full node
        fullMetadataNode.put("operatorType", operatorType);
        fullMetadataNode.set("jsonSchema", schemaNode);
        fullMetadataNode.set("additionalMetadata", additionalMetadataNode);

        Files.write(operatorSchemaPath, objectMapper.writeValueAsBytes(fullMetadataNode));
        
        System.out.println("generating schema of " + operatorType + " completed");
        System.out.println(fullMetadataNode);
    }
    
    public static Path getJsonSchemaPath(Class<? extends PredicateBase> predicateClass) {
        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);
        
        // find the operator directory by its predicate class
        Path classDirectory = Utils.getTexeraHomePath()
            .resolve(DataConstants.TexeraProject.TEXERA_DATAFLOW.getProjectName())
            .resolve("src/main/java")
            .resolve(predicateClass.getPackage().getName().replace('.', '/'));
        
        Path operatorSchemaPath = classDirectory.resolve(operatorType + "Schema.json");
        
        return operatorSchemaPath;
    }
    
    public static List<String> getRequiredProperties(Class<? extends PredicateBase> predicateClass) {
        ArrayList<String> requiredProperties = new ArrayList<>();
        
        Constructor<?> constructor = getJsonCreatorConstructor(predicateClass);
        
        for (Annotation[] annotations : Arrays.asList(constructor.getParameterAnnotations())) {
            // find the @JsonProperty annotation for each parameter
            Optional<Annotation> findJsonProperty = Arrays.asList(annotations).stream()
                    .filter(annotation -> annotation.annotationType().equals(JsonProperty.class)).findAny();
            if (! findJsonProperty.isPresent()) {
                continue;
            }
            // add the required property to the list
            JsonProperty jsonProperty = (JsonProperty) findJsonProperty.get();
            if (jsonProperty.required()) {
                requiredProperties.add(jsonProperty.value());
            }
        }
        return requiredProperties;
    }
    
    public static List<String> getAdvancedOptionProperties(Class<? extends PredicateBase> predicateClass) {
        ArrayList<String> advancedProperties = new ArrayList<>();
        
        Constructor<?> constructor = getJsonCreatorConstructor(predicateClass);
        
        for (Annotation[] annotations : Arrays.asList(constructor.getParameterAnnotations())) {
            // find the @AdvancedOption annotation for each parameter
            Optional<Annotation> findAdvancedOptionAnnotation = Arrays.asList(annotations).stream()
                    .filter(annotation -> annotation.annotationType().equals(AdvancedOption.class)).findAny();
            if (! findAdvancedOptionAnnotation.isPresent()) {
                continue;
            }
            AdvancedOption advancedOptionAnnotation = (AdvancedOption) findAdvancedOptionAnnotation.get();
            
            // find the @JsonProperty annotation
            Optional<Annotation> findJsonProperty = Arrays.asList(annotations).stream()
                    .filter(annotation -> annotation.annotationType().equals(JsonProperty.class)).findAny();
            if (! findJsonProperty.isPresent()) {
                continue;
            }
            JsonProperty jsonProperty = (JsonProperty) findJsonProperty.get();
            
            if (advancedOptionAnnotation.isAdvancedOption()) {
                advancedProperties.add(jsonProperty.value());
            }
        }
        
        return advancedProperties;
    }
    
    public static Map<String, Object> getPropertyDefaultValues(Class<? extends PredicateBase> predicateClass) {
        HashMap<String, Object> defaultValueMap = new HashMap<>();
        
        Constructor<?> constructor = getJsonCreatorConstructor(predicateClass);
        
        // get all parameter types
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            // find the @JsonProperty annotation for each parameter
            Annotation[] annotations = constructor.getParameterAnnotations()[i];
            Optional<Annotation> findJsonProperty = Arrays.asList(annotations).stream()
                    .filter(annotation -> annotation.annotationType().equals(JsonProperty.class)).findAny();
            if (! findJsonProperty.isPresent()) {
                continue;
            }
            // convert the defaultValue from the string to the parameter's type
            JsonProperty jsonProperty = (JsonProperty) findJsonProperty.get();
            if (! jsonProperty.defaultValue().trim().isEmpty()) {
                defaultValueMap.put(jsonProperty.value(), 
                        objectMapper.convertValue(jsonProperty.defaultValue(), parameterTypes[i]));
            }
        }
        
        return defaultValueMap;
    }
    
    public static Constructor<?> getJsonCreatorConstructor(Class<? extends PredicateBase> predicateClass) {
        // find the constrcutor with @JsonCreator annotation
        Optional<Constructor<?>> findJsonCreator = Arrays.asList(predicateClass.getConstructors()).stream()
                .filter(constructor -> constructor.getAnnotation(JsonCreator.class) != null).findAny();
        if (! findJsonCreator.isPresent()) {
            throw new TexeraException(predicateClass + ": json creator constructor is not present");
        }
        return findJsonCreator.get();
    }

    // implementation adapted from https://gist.github.com/tedpennings/6253541
    public static void searchForEntity(JsonNode node, String entityName, Set<JsonNode> results) {
        // A naive depth-first search implementation using recursion. Useful
        // **only** for small object graphs. This will be inefficient
        // (stack overflow) for finding deeply-nested needles or needles
        // toward the end of a forest with deeply-nested branches.
        if (node == null) {
            return;
        }
        if (node.has(entityName)) {
            results.add(node.get(entityName));
        }
        if (!node.isContainerNode()) {
            return;
        }
        for (JsonNode child : node) {
            if (child.isContainerNode()) {
                searchForEntity(child, entityName, results);
            }
        }
    }

}
