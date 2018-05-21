package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class RegexSourcePredicate extends RegexPredicate {
    
    private final String tableName;
    private final Boolean useIndex;
    
    /*
     * This constructor is for internal use. It's not a JSON entry point.
     */
    public RegexSourcePredicate(
            String regex, 
            List<String> attributeNames, 
            String tableName,
            String spanListName) {
        this(regex, attributeNames, null, tableName, null, spanListName);
    }

    /**
     * RegexSourcePredicate is used to create a RegexSourceOperator.
     * 
     * @param regex, the regex to be used
     * @param attributeNames, a list of attribute names to match regex on
     * @param ignoreCase, optional, ignores regex case, default false
     * @param tableName, the name of the source table
     * @param useIndex, optional, use the gram-based regex index query, default true
     * @param spanListName, the name of the attribute where the results will be put in
     */
    @JsonCreator
    public RegexSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.REGEX, required = true)
            String regex, 
            
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames, 
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.REGEX_IGNORE_CASE, required = false,
                    defaultValue = "false")
            Boolean ignoreCase, 
            
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.REGEX_USE_INDEX, required = false, 
                    defaultValue = "false")
            Boolean useIndex,
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        super(regex, attributeNames, ignoreCase, spanListName);

        if (tableName == null || tableName.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.tableName = tableName;
        if (useIndex == null) {
            this.useIndex = false;
        } else {
            this.useIndex = useIndex;
        }
    }
    
    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @JsonProperty(PropertyNameConstants.REGEX_USE_INDEX)
    public Boolean isUseIndex() {
        return this.useIndex;
    }
    
    @Override
    public RegexMatcherSourceOperator newOperator() {
        return new RegexMatcherSourceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Regex")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Perform an index-based search on a table using a regular expression")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }

}
