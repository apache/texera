package edu.uci.ics.textdb.jsonplangen;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.PlanGenException;

/**
 * OperatorBuilder is a base abstract class for building an operator based on
 * its properties. Subclasses need to implement the build() function, which
 * builds the IOperator object to return.
 * 
 * This abstract class defines some commonly used variables:  
 *   operatorID: a string representing operator's ID
 *   operatorProperties: a map of key-value pairs representing the operator's properties
 *  
 * OperatorBuilder currently offers helper functions for the following properties:
 *   ATTRIBUTE_NAMES (required to generate attribute list)
 *   ATTRIBUTE_TYPES (required to generate attribute list)
 *   LIMIT  (often optional)
 *   OFFSET (often optional)
 * 
 * @author Zuozhi Wang
 *
 */
public abstract class OperatorBuilder {

    protected String operatorID;
    protected Map<String, String> operatorProperties;

    public static final String ATTRIBUTE_NAMES = "attributeNames";
    public static final String ATTRIBUTE_TYPES = "attributeTypes";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";

    /**
     * Construct an OperatorBulider with operatorID and operatorProperties
     * 
     * @param operatorID
     * @param operatorProperties
     */
    public OperatorBuilder(String operatorID, Map<String, String> operatorProperties) {
        this.operatorID = operatorID;
        this.operatorProperties = operatorProperties;
    }

    public abstract IOperator build() throws Exception;

    /**
     * This function returns a required property. An exception is thrown
     * if the operator properties don't contain the key.
     * 
     * @param key
     * @return value
     * @throws PlanGenException, if the operator properties do not contain the key.
     */
    protected String getRequiredProperty(String key) throws PlanGenException {
        if (operatorProperties.containsKey(key)) {
            return operatorProperties.get(key);
        } else {
            throw new PlanGenException(operatorID + " missing required key " + key);
        }
    }

    /**
     * This function returns an optional property. Null will be returned
     * if the operator properties don't contain the key.
     * 
     * @param key
     * @return value, null if the operator properties do not contain the key.
     */
    protected String getOptionalProperty(String key) {
        return operatorProperties.get(key);
    }

    /**
     * This function finds properties related to constructing the attributes in
     * operatorProperties, and converts them to a list of attributes.
     * 
     * @return a list of attributes
     * @throws PlanGenException
     */
    protected List<Attribute> constructAttributeList() throws PlanGenException {
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES);
        String attributeTypesStr = getRequiredProperty(ATTRIBUTE_TYPES);

        List<String> attributeNames = splitAttributes(attributeNamesStr);
        List<String> attributeTypes = splitAttributes(attributeTypesStr);

        JsonPlanGenUtils.planGenAssert(attributeNames.size() == attributeTypes.size(), "attribute names and attribute types are not coherent");
        JsonPlanGenUtils.planGenAssert(attributeTypes.stream().allMatch(typeStr -> JsonPlanGenUtils.isValidAttributeType(typeStr))
                ,"attribute type is not valid");

        List<Attribute> attributeList = IntStream.range(0, attributeNames.size()) // for each index in the list
                .mapToObj(i -> constructAttribute(attributeNames.get(i), attributeTypes.get(i))) // construct an attribute
                .collect(Collectors.toList());

        return attributeList;
    }

    private Attribute constructAttribute(String attributeNameStr, String attributeTypeStr) {
        FieldType fieldType = FieldType.valueOf(attributeTypeStr.toUpperCase());
        return new Attribute(attributeNameStr, fieldType);
    }

    private List<String> splitAttributes(String attributesStr) {
        String[] attributeArray = attributesStr.split(",");
        return Arrays.asList(attributeArray).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * This function finds the "limit" value in the operator's properties.
     * It returns null if the value is not found.
     * 
     * @return limit, null if not found
     * @throws PlanGenException
     */
    protected Integer findLimit() throws PlanGenException {
        String limitStr = getOptionalProperty(LIMIT);
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
    protected Integer findOffset() throws PlanGenException {
        String offsetStr = getOptionalProperty(OFFSET);
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
