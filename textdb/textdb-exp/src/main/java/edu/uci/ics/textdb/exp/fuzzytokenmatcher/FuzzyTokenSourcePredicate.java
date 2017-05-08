package edu.uci.ics.textdb.exp.fuzzytokenmatcher;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class FuzzyTokenSourcePredicate extends FuzzyTokenPredicate {
    
    private final String tableName;

    public FuzzyTokenSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_QUERY, required = true)
            String query, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true)
            String luceneAnalyzerStr,
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_THRESHOLD_RATIO, required = true)
            Double thresholdRatio,
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        super(query, attributeNames, luceneAnalyzerStr, thresholdRatio, spanListName);
        this.tableName = tableName;
    }
    
    @JsonProperty(value = PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public IOperator newOperator() {
        return new FuzzyTokenMatcherSourceOperator(this);
    }

}
