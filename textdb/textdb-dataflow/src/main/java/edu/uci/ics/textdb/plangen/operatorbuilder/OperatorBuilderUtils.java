package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.exception.PlanGenException;

public class OperatorBuilderUtils {
    
    public static final String ATTRIBUTE_NAMES = "attributeNames";
    public static final String ATTRIBUTE_TYPES = "attributeTypes";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    
    public static String DATA_SOURCE = "data_source";
    
    
    /**
     * This function returns a required property. An exception is thrown
     * if the operator properties don't contain the key.
     * 
     * @param key
     * @return value
     * @throws PlanGenException, if the operator properties do not contain the key.
     */
    public static String getRequiredProperty(String key, Map<String, String> operatorProperties) throws PlanGenException {
        if (! operatorProperties.containsKey(key)) {
            throw new PlanGenException("Required key missing: " + key);
        }
        if (operatorProperties.get(key).trim().isEmpty()) {
            throw new PlanGenException("Required key is empty: " + key);
        }
        return operatorProperties.get(key);
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
