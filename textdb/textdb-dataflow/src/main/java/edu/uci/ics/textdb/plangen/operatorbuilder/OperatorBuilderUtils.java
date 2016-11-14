package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONObject;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.DataStore;

public class OperatorBuilderUtils {
    
    public static final String ATTRIBUTE_NAMES = "attributeNames";
    public static final String ATTRIBUTE_TYPES = "attributeTypes";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    
    public static String DATA_DIRECTORY = "directory";
    public static String SCHEMA = "schema";
    
    
    /**
     * This function returns a required property. An exception is thrown
     * if the operator properties don't contain the key.
     * 
     * @param key
     * @return value
     * @throws PlanGenException, if the operator properties do not contain the key.
     */
    public static String getRequiredProperty(String key, Map<String, String> operatorProperties) throws PlanGenException {
        if (operatorProperties.containsKey(key)) {
            return operatorProperties.get(key);
        } else {
            throw new PlanGenException("Required key missing: " + key);
        }
    }

    /**
     * This function returns an optional property. Null will be returned
     * if the operator properties don't contain the key.
     * 
     * @param key
     * @return value, null if the operator properties do not contain the key.
     */
    public static String getOptionalProperty(String key, Map<String, String> operatorProperties) {
        return operatorProperties.get(key);
    }
    
    /**
     * This function constructs a DataStore from operator properties.
     * 
     * It currently needs the following properties from operatorProperties:
     * 
     *   directory: the directory of data store
     *   schema: schema of data store, which is a JSON-format string. 
     *   "attributeNames" key's value is a string of comma-separated attribute names, 
     *   "attirbuteTypes" key's value is a string of comma-separated attribute types corresponding to attribute names.
     * 
     * Here's a sample JSON representation of these properties:
     * 
     * {
     *   "directory" : "directoryOfDatastore",
     *   "schema" : {
     *     "attributeNames" : "attribute1Name, attribute2Name, attribute3Name",
     *     "attributeTypes" : "integer, string, text"
     *   }
     * }
     * 
     * @param operatorProperties
     * @return dataStore, dataStore constructed according to directory and schema.
     * @throws PlanGenException
     */
    public static DataStore constructDataStore(Map<String, String> operatorProperties) throws PlanGenException {
        String directoryStr = OperatorBuilderUtils.getRequiredProperty(DATA_DIRECTORY, operatorProperties);
        String schemaStr = OperatorBuilderUtils.getRequiredProperty(SCHEMA, operatorProperties);
        
        JSONObject schemaJsonObject = new JSONObject(schemaStr);
        
        String attributeNamesStr = schemaJsonObject.getString(ATTRIBUTE_NAMES);
        String attributeTypesStr = schemaJsonObject.getString(ATTRIBUTE_TYPES);
        
        List<String> attributeNames = splitStringByComma(attributeNamesStr);
        List<String> attributeTypes = splitStringByComma(attributeTypesStr);

        PlanGenUtils.planGenAssert(attributeNames.size() == attributeTypes.size(), "attribute names and attribute types are not coherent");
        PlanGenUtils.planGenAssert(attributeTypes.stream().allMatch(typeStr -> PlanGenUtils.isValidAttributeType(typeStr))
                ,"attribute type is not valid");

        List<Attribute> attributeList = IntStream.range(0, attributeNames.size()) // for each index in the list
                .mapToObj(i -> constructAttribute(attributeNames.get(i), attributeTypes.get(i))) // construct an attribute
                .collect(Collectors.toList());

        Schema schema = new Schema(attributeList.stream().toArray(Attribute[]::new));
        
        DataStore dataStore = new DataStore(directoryStr, schema);
        
        return dataStore;
    }

    /**
     * This function finds properties related to constructing the attributes in
     * operatorProperties, and converts them to a list of attributes.
     * 
     * It currently needs the following properties from operatorProperties: 
     *   attributeNames: a list of attributes' names (separated by comma)
     *   attributeTypes: a list of attributes' types corresponding to attribute names (separated by comma)
     *   
     * Here's a sample JSON representation of these properties:
     * 
     * {
     *   "attributeNames" : "attribute1Name, attribute2Name, attribute3Name",
     *   "attributeTypes" : "attribute1Type, attribute2Type, attribute3Type"
     * }
     * 
     * TODO: this function should be deleted after all attributeLists are changed to attributeNames
     * 
     * @param operatorProperties
     * @return a list of attributes
     * @throws PlanGenException
     */
    public static List<Attribute> constructAttributeList(Map<String, String> operatorProperties) throws PlanGenException {
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES, operatorProperties);
        String attributeTypesStr = getRequiredProperty(ATTRIBUTE_TYPES, operatorProperties);

        List<String> attributeNames = splitStringByComma(attributeNamesStr);
        List<String> attributeTypes = splitStringByComma(attributeTypesStr);

        PlanGenUtils.planGenAssert(attributeNames.size() == attributeTypes.size(), "attribute names and attribute types are not coherent");
        PlanGenUtils.planGenAssert(attributeTypes.stream().allMatch(typeStr -> PlanGenUtils.isValidAttributeType(typeStr))
                ,"attribute type is not valid");

        List<Attribute> attributeList = IntStream.range(0, attributeNames.size()) // for each index in the list
                .mapToObj(i -> constructAttribute(attributeNames.get(i), attributeTypes.get(i))) // construct an attribute
                .collect(Collectors.toList());

        return attributeList;
    }
    
    /**
     * This function finds properties related to constructing the attribute names in
     * operatorProperties, and converts them to a list of attribute names.
     * 
     * It currently needs the following properties from operatorProperties: 
     *   attributeNames: a list of attributes' names (separated by comma)
     *   
     * Here's a sample JSON representation of these properties:
     * 
     * {
     *   "attributeNames" : "attribute1Name, attribute2Name, attribute3Name"
     * }
     * 
     * @param operatorProperties
     * @return a list of attribute names
     * @throws PlanGenException
     */
    public static List<String> constructAttributeNames(Map<String, String> operatorProperties) throws PlanGenException {
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES, operatorProperties);

        List<String> attributeNames = splitStringByComma(attributeNamesStr);

        return attributeNames;
    }

    private static Attribute constructAttribute(String attributeNameStr, String attributeTypeStr) {
        FieldType fieldType = FieldType.valueOf(attributeTypeStr.toUpperCase());
        return new Attribute(attributeNameStr, fieldType);
    }

    public static List<String> splitStringByComma(String str) {
        String[] strArray = str.split(",");
        return Arrays.asList(strArray).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * This function finds the "limit" value in the operator's properties.
     * It returns null if the value is not found.
     * 
     * @return limit, null if not found
     * @throws PlanGenException
     */
    public static Integer findLimit(Map<String, String> operatorProperties) throws PlanGenException {
        String limitStr = getOptionalProperty(LIMIT, operatorProperties);
        if (limitStr == null) {
            return null;
        }
        Integer limit = Integer.parseInt(limitStr);
        if (limit < 0) {
            throw new PlanGenException("Limit must be equal to or greater than 0");
        }
        return limit;
    }

    /**
     * This function finds the "offset" value in the operator's properties.
     * It returns null if the value is not found.
     * 
     * @return offset, null if not found
     * @throws PlanGenException
     */
    public static Integer findOffset(Map<String, String> operatorProperties) throws PlanGenException {
        String offsetStr = getOptionalProperty(OFFSET, operatorProperties);
        if (offsetStr == null) {
            return null;
        }
        Integer offset = Integer.parseInt(offsetStr);
        if (offset < 0) {
            throw new PlanGenException("Offset must be equal to or greater than 0");
        }
        return offset;
    }
    
}
