package edu.uci.ics.texera.dataflow.fuzzytokenmatcher;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class FuzzyTokenSourcePredicate extends FuzzyTokenPredicate {
    
    private final String tableName;

    @JsonCreator
    public FuzzyTokenSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_QUERY, required = true)
            String query, 
            
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true,
                    defaultValue = LuceneAnalyzerConstants.STANDARD_ANALYZER)
            String luceneAnalyzerStr,
            
            @JsonProperty(value = PropertyNameConstants.FUZZY_TOKEN_THRESHOLD_RATIO, required = true)
            Double thresholdRatio,
            
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = true)
            String spanListName) {
        super(query, attributeNames, luceneAnalyzerStr, thresholdRatio, spanListName);

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
    public FuzzyTokenMatcherSourceOperator newOperator() {
        return new FuzzyTokenMatcherSourceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Fuzzy Token")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Perform an index-based search on a table for records similar to given tokens")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }

}
