package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

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
            @JsonProperty(value = PropertyNameConstants.REGEX_IGNORE_CASE, required = false)
            Boolean ignoreCase, 
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.REGEX_USE_INDEX, required = false)
            Boolean useIndex,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        super(regex, attributeNames, ignoreCase, spanListName);
        this.tableName = tableName;
        if (useIndex == null) {
            this.useIndex = true;
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
    public IOperator newOperator() {
        return new RegexMatcherSourceOperator(this);
    }

}
