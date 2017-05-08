package edu.uci.ics.textdb.exp.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

/*
 * @author varun bharill, parag saraogi
 * 
 * This class builds the query to perform boolean searches in a lucene index. 
 * The threshold for boolean searches is taken input as a ratio with is converted to integer. 
 * In the worst case if this integer becomes 0, we will set it to 1. 
 */
public class FuzzyTokenPredicate extends PredicateBase {

    private final String query;
    private final List<String> attributeNames;
    private final String luceneAnalyzerStr;
    private final Double thresholdRatio;
    private final String spanListName;
    
    // fields not included in json properties
    private final List<String> queryTokens;
    private final Integer threshold;

    public FuzzyTokenPredicate(
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_QUERY, required = true)
            String query, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true)
            String luceneAnalyzerStr,
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_THRESHOLD_RATIO, required = true)
            Double thresholdRatio,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        this.query = query;
        this.attributeNames = attributeNames;
        this.luceneAnalyzerStr = luceneAnalyzerStr;
        this.thresholdRatio = thresholdRatio;
        
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = this.getID();
        } else {
            this.spanListName = spanListName.trim();
        }
        
        this.queryTokens = DataflowUtils.tokenizeQuery(this.luceneAnalyzerStr, this.query);
        this.threshold = computeThreshold(this.thresholdRatio, queryTokens.size());
    }

    @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_QUERY)
    public String getQuery() {
        return this.query;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(this.attributeNames);
    }

    @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerStr() {
        return this.luceneAnalyzerStr;
    }

    @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_THRESHOLD_RATIO)
    public Double getThresholdRatio() {
        return this.thresholdRatio;
    }
    
    @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
    public String getSpanListName() {
        return this.spanListName;
    }
    
    @JsonIgnore
    protected Collection<String> getQueryTokens() {
        return this.queryTokens;
    }
    
    @JsonIgnore
    protected Integer getThreshold() {
        return this.threshold;
    }
    
    /*
     * The input threshold given by the end-user (thresholdRatio data member) is
     * a ratio but boolean search query requires integer as a threshold. In case
     * if the the threshold becomes 0, we will set it by default to 1.
     */
    @JsonIgnore
    public static int computeThreshold(double thresholdRatio, int tokenSize) {
        int threshold = (int) (thresholdRatio * tokenSize);
        if (threshold == 0) {
            threshold = 1;
        }
        return threshold; 
    }
    
    @Override
    public IOperator newOperator() {
        return new FuzzyTokenMatcher(this);
    }

}
