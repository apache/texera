package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;


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
    private final Boolean ignoreCase;
    
    
    /*
     * This constructor is only for internal use.
     */
    public RegexPredicate(String regex, List<String> attributeNames) {
        this(regex, attributeNames, null);
    }

    /**
     * RegexPredicate is used to create a RegexMatcher.
     * 
     * @param regex, the regex to be used
     * @param attributeNames, a list of attribute names to match regex on
     * @param ignoreCase, optional, ignores regex case, default false
     */
    @JsonCreator
    public RegexPredicate(
            @JsonProperty(value = PropertyNameConstants.REGEX, required = true)
            String regex, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.REGEX_IGNORE_CASE, required = false)
            Boolean ignoreCase) {
        this.regex = regex;
        this.attributeNames = attributeNames;
        if (ignoreCase == null) {
            this.ignoreCase = false;
        } else {
            this.ignoreCase = ignoreCase;
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
    
    @JsonProperty(PropertyNameConstants.REGEX_IGNORE_CASE)
    public Boolean isIgnoreCase() {
        return this.ignoreCase;
    }
    
    @Override
    public IOperator newOperator() {
        return new RegexMatcher(this);
    }

}
