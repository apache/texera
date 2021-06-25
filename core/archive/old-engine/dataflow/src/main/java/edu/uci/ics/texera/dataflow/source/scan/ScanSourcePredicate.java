package edu.uci.ics.texera.dataflow.source.scan;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * ScanSourcePredicate is used by ScanBasedSourceOperator
 * 
 * @author Zuozhi Wang
 *
 */
public class ScanSourcePredicate extends PredicateBase {
    
    private final String tableName;
    
    @JsonCreator
    public ScanSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.TABLE_NAME, required=true)
            String tableName) {

        if (tableName == null || tableName.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.tableName = tableName;
    }

    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public ScanBasedSourceOperator newOperator() {
        return new ScanBasedSourceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Scan")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Read records from a table one by one")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }
    
}
