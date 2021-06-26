package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
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

        if (tableName == null || tableName.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.tableName = tableName;
    }
    
    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public KeywordMatcherSourceOperator newOperator() {
        return new KeywordMatcherSourceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Keyword")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Perform an index-based search on a table using a keyword")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }

}
