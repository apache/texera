package edu.uci.ics.texera.dataflow.wordcount;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountOperatorPredicate extends PredicateBase {
        
    private final String attribute;
    private final String analyzer;
    
    @JsonCreator
    public WordCountOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attribute,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true,
                    defaultValue = LuceneAnalyzerConstants.STANDARD_ANALYZER)
            String analyzer) {
        
        this.attribute = attribute;
        this.analyzer = analyzer;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttribute() {
        return this.attribute;
    }
    
    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerString() {
        return this.analyzer;
    }

    @Override
    public WordCountOperator newOperator() {
        return new WordCountOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Word Count")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Count the frequency of each word in all the documents")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }
    
}
