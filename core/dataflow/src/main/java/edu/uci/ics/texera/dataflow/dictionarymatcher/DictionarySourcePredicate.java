package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;

public class DictionarySourcePredicate extends DictionaryPredicate {
    
    private final String tableName;
    
    /**
     * DictionarySourcePredicate is used to create a DictionarySourceOperator.
     * 
     * @param dictionary, the dictionary to be used
     * @param attributeNames, the names of the attributes to match the dictionary
     * @param luceneAnalyzerStr, the lucene analyzer to tokenize the dictionary entries
     * @param keywordMatchingType, the keyword matching type ({@code KeywordMatchingType}
     * @param tableName, the name of the source table
     */
    @JsonCreator
    public DictionarySourcePredicate(
            @JsonUnwrapped
            Dictionary dictionary, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames, 
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true)
            String luceneAnalyzerStr,
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = true)
            KeywordMatchingType keywordMatchingType,
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {
        super(dictionary, attributeNames, luceneAnalyzerStr, keywordMatchingType, spanListName);
        this.tableName = tableName;
    }
    
    @JsonProperty(value = PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public IOperator newOperator() {
        return new DictionaryMatcherSourceOperator(this);
    }
    
}
