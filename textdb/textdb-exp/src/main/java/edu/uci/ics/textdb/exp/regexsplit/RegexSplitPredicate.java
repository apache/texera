package edu.uci.ics.textdb.exp.regexsplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate extends PredicateBase {
    
    public enum SplitType {
        GROUP_LEFT("left"), // the regex is grouped with the text on its left

        GROUP_RIGHT("right"), // the regex is grouped with the text on its right

        STANDALONE("standalone"); // the regex becomes a standalone tuple
        
        private final String name;
        
        private SplitType(String name) {
            this.name = name;
        }
        
        // use the name string instead of enum string in JSON
        @JsonValue
        public String getName() {
            return this.name;
        }
    }
    
    private final String splitRegex;
    private final String splitAttribute;
    private final SplitType splitType;
    
    /**
     * Construct a RegexSplitPredicate.
     * 
     * @param regex, the regex query
     * @param attributeToSplit, the attribute name to perform split operation on
     * @param splitType, a type to indicate where the regex pattern merge into. 
     */
    @JsonCreator
    public RegexSplitPredicate(
            @JsonProperty(value=PropertyNameConstants.SPLIT_REGEX, required=true)
            String splitRegex,
            @JsonProperty(value=PropertyNameConstants.SPLIT_ATTRIBUTE, required=true)
            String splitAttribute,
            @JsonProperty(value=PropertyNameConstants.SPLIT_TYPE, required=true)
            SplitType splitType ) {
        this.splitRegex = splitRegex;
        this.splitAttribute = splitAttribute;
        this.splitType = splitType;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_REGEX)
    public String getRegex() {
        return splitRegex;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_ATTRIBUTE)
    public String getAttributeToSplit() {
        return splitAttribute;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_TYPE)
    public SplitType getSplitType() {
        return splitType;
    }
    
    @Override
    public IOperator newOperator() {
        return new RegexSplitOperator(this);
    }
    
}
