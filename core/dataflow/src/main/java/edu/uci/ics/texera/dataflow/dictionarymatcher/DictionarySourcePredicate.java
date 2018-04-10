package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

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
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true,
                    defaultValue = LuceneAnalyzerConstants.STANDARD_ANALYZER)
            String luceneAnalyzerStr,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = true,
                    defaultValue = KeywordMatchingType.KeywordMatchingTypeName.PHRASE)
            KeywordMatchingType keywordMatchingType,
            
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {

        super(dictionary, attributeNames, luceneAnalyzerStr, keywordMatchingType, spanListName);

        if (tableName == null || tableName.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.tableName = tableName;
    }
    
    @JsonProperty(value = PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public DictionaryMatcherSourceOperator newOperator() {
        return new DictionaryMatcherSourceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Dictionary")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Perform an index-based search on a table using a dictionary")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }   
    
}
