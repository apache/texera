package edu.uci.ics.texera.dataflow.sink.json;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class JSONSinkPredicate extends PredicateBase {

    private Integer limit;
    private Integer offset;
    
    public JSONSinkPredicate() {
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
    };
    
    @JsonCreator
    public JSONSinkPredicate(
            @JsonProperty(value = PropertyNameConstants.LIMIT, required = false)
            Integer limit,
            @JsonProperty(value = PropertyNameConstants.OFFSET, required = false)
            Integer offset
            ) {
        this.limit = limit;
        if (this.limit == null) {
            this.limit = Integer.MAX_VALUE;
        }
        this.offset = offset;
        if (this.offset == null) {
            this.offset = 0;
        }
    }
    
    @JsonProperty(value = PropertyNameConstants.LIMIT)
    public Integer getLimit() {
        return this.limit;
    }
    
    @JsonProperty(value = PropertyNameConstants.OFFSET)
    public Integer getOffset() {
        return this.offset;
    }
    
    @Override
    public JSONSink newOperator() {
        return new JSONSink(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Write to JSON file")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Write the results to an JSON file")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.DATABASE_GROUP)
            .build();
    }

}
