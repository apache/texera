package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * KeywordSourcePredicate is the predicate used by KeywordMatcherSourceOperator.
 * This predicate is based on KeywordPredicate, with an additional member "tableName".
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordSourcePredicate extends KeywordPredicate {
    
    private final String tableName;
    
    //TODO find a way to not write duplicate annotations (for fields declared in superclass)
    @JsonCreator
    public KeywordSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.KEYWORD_QUERY, required = true)
            String query,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = false)
            String luceneAnalyzerString, 
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = false)
            KeywordMatchingType matchingType,
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {
        
        super(query, attributeNames, luceneAnalyzerString, matchingType, spanListName);
        this.tableName = tableName;
    }
    
    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public IOperator newOperator() {
        return new KeywordMatcherSourceOperator(this);
    }

}

