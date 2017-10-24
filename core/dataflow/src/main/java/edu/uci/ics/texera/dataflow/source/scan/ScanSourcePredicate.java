package edu.uci.ics.texera.dataflow.source.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.PlanGenException;
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
            throw new PlanGenException(PropertyNameConstants.EMPTY_NAME_EXCETION);
        }
        this.tableName = tableName;
    }

    @JsonProperty(PropertyNameConstants.TABLE_NAME)
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public IOperator newOperator() {
        return new ScanBasedSourceOperator(this);
    }
    
}
