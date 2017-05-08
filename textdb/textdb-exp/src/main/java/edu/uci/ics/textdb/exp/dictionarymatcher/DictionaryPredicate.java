package edu.uci.ics.textdb.exp.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;

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
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true)
            String luceneAnalyzerStr,
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = true)
            KeywordMatchingType keywordMatchingType,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {

        this.dictionary = dictionary;
        this.luceneAnalyzerStr = luceneAnalyzerStr;
        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;
        
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = this.getID();
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
    
}
