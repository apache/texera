package Engine.SchemaSupport.constants;


import Engine.SchemaSupport.schema.Schema;

/**
 * @author Zuozhi Wang
 * @author sandeepreddy602
 *
 */
public class ErrorMessages {

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
