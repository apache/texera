package edu.uci.ics.texera.api.constants;

import edu.uci.ics.texera.api.schema.Schema;

/**
 * @author Zuozhi Wang
 * @author sandeepreddy602
 *
 */
public class ErrorMessages {
    public static final String OPERATOR_NOT_OPENED = "The operator is not opened";
    public static final String SCHEMA_CANNOT_BE_NULL = "Schema cannot be null or empty";
    public static final String INPUT_OPERATOR_NOT_SPECIFIED = "Input operator is not specified";
    public static final String INPUT_OPERATOR_CHANGED_AFTER_OPEN = "Input Operator cannot be changed after opening the operator";
    public static final String NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH = "Number of arguments does not match. Expected: %d, Given: %d";
    public static final String INVALID_INPUT_SCHEMA_FOR_SOURCE = "Source operator should not have input schema";
    public static final String INVALID_OUTPUT_SCHEMA_FOR_SINK = "Sink operator should not have output schema";
    public static final String INVALID_FUNCTION_CALL = "Reader and Writer do not have input and output schema";


    public static final String DUPLICATE_ATTRIBUTE(Schema schema, String attributeName) {
        return DUPLICATE_ATTRIBUTE(schema.getAttributeNames(), attributeName);
    }
    
    public static final String DUPLICATE_ATTRIBUTE(Iterable<String> attributeNameList, String attributeName) {
        return String.format("attribute %s already exists in the Schema: %s", 
                attributeName, attributeNameList);
    }
    
    public static final String ATTRIBUTE_NOT_EXISTS(Schema schema, String attributeName) {
        return ATTRIBUTE_NOT_EXISTS(schema.getAttributeNames(), attributeName);
    }
    
    public static final String ATTRIBUTE_NOT_EXISTS(Iterable<String> attributeNameList, String attributeName) {
        return String.format("attribute %s does not exist in the schema: %s", 
                attributeName, attributeNameList);
    }
}
