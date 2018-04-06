package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class DictionaryPredicate extends PredicateBase {

    private final Dictionary dictionary;
    private final List<String> attributeNames;
    private final String luceneAnalyzerStr;
    private final KeywordMatchingType keywordMatchingType;
    private final String spanListName;

    /**
     * DictionaryPredicate is used to create a DictionaryMatcher.
     * 
     * @param dictionary, the dictionary to be used
     * @param attributeNames, the names of the attributes to match the dictionary
     * @param luceneAnalyzerStr, the lucene analyzer to tokenize the dictionary entries
     * @param keywordMatchingType, the keyword matching type ({@code KeywordMatchingType}
     * @param spanListName, optional, the name of the attribute where the results (a list of spans) will be in, 
     *          default value is the id of the predicate
     */
    @JsonCreator
    public DictionaryPredicate(
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
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {
        
        this.dictionary = dictionary;
        this.luceneAnalyzerStr = luceneAnalyzerStr;
        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;
        
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = null;
        } else {
            this.spanListName = spanListName.trim();
        }
    }
    
    @JsonUnwrapped
    @JsonProperty(value = PropertyNameConstants.DICTIONARY, required = true)
    public Dictionary getDictionary() {
        return dictionary;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(attributeNames);
    }

    @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getAnalyzerString() {
        return luceneAnalyzerStr;
    }
    
    @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE)
    public KeywordMatchingType getKeywordMatchingType() {
        return keywordMatchingType;
    }
    
    @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME)
    public String getSpanListName() {
        return spanListName;
    }
    
    @Override
    public IOperator newOperator() {
        return new DictionaryMatcher(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Dictionary Search")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Search the documents using a dictionary (multiple keywords)")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SEARCH_GROUP)
            .build();
    }
    
}
