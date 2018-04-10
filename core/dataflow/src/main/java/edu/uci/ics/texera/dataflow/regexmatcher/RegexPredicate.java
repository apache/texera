package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;


/**
 * This class is the predicate for Regex.
 * 
 * @author Zuozhi Wang
 * @author Shuying Lai
 *
 */
public class RegexPredicate extends PredicateBase {

    private final String regex;
    private final List<String> attributeNames;
    private final String spanListName;
    private final Boolean ignoreCase;
    
    /*
     * This constructor is only for internal use.
     */
    public RegexPredicate(String regex, List<String> attributeNames, String spanListName) {
        this(regex, attributeNames, null, spanListName);
    }

    /**
     * RegexPredicate is used to create a RegexMatcher.
     * 
     * @param regex, the regex to be used
     * @param attributeNames, a list of attribute names to match regex on
     * @param ignoreCase, optional, ignores regex case, default false
     * @param spanListName, the name of the attribute where the results will be put in
     */
    @JsonCreator
    public RegexPredicate(
            @JsonProperty(value = PropertyNameConstants.REGEX, required = true)
            String regex, 
            
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.REGEX_IGNORE_CASE, required = false,
                    defaultValue = "false")
            Boolean ignoreCase,
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {
        
        if (regex.trim().isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_REGEX_EXCEPTION);
        }
        
        this.regex = regex;
        this.attributeNames = attributeNames;
        if (ignoreCase == null) {
            this.ignoreCase = false;
        } else {
            this.ignoreCase = ignoreCase;
        }
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = null;
        } else {
            this.spanListName = spanListName.trim();
        }
    }

    @JsonProperty(PropertyNameConstants.REGEX)
    public String getRegex() {
        return this.regex;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(this.attributeNames);
    }
    
    @JsonProperty(PropertyNameConstants.SPAN_LIST_NAME)
    public String getSpanListName() {
        return this.spanListName;
    }
    
    @JsonProperty(PropertyNameConstants.REGEX_IGNORE_CASE)
    public Boolean isIgnoreCase() {
        return this.ignoreCase;
    }
    
    @Override
    public IOperator newOperator() {
        return new RegexMatcher(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Regex Match")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Search the documents using a regular expression")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SEARCH_GROUP)
            .build();
    }

}
