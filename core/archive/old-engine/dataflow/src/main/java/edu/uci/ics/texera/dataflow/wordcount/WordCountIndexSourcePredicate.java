package edu.uci.ics.texera.dataflow.wordcount;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountIndexSourcePredicate extends PredicateBase {

    private final String tableName;
    private final String attribute;
    
    @JsonCreator
    public WordCountIndexSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attribute) {

        if (tableName == null || tableName.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.tableName = tableName;
        this.attribute = attribute;
        
    }
    
    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttribute() {
        return this.attribute;
    }

    @Override
    public WordCountIndexSource newOperator() {
        return new WordCountIndexSource(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Word Count")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Count the frequency of for each word using index")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }
    
}
