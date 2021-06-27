package edu.uci.ics.texera.dataflow.sink.tuple;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class TupleSinkPredicate extends PredicateBase {

    private Integer limit;
    private Integer offset;

    public TupleSinkPredicate() {
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
    };

    @JsonCreator
    public TupleSinkPredicate(
            @JsonProperty(value = PropertyNameConstants.LIMIT, required = false, defaultValue = "10")
            Integer limit,
            @JsonProperty(value = PropertyNameConstants.OFFSET, required = false, defaultValue = "0")
            Integer offset
            ) {

        if (limit != null && limit < 0) {
            throw new TexeraException(PropertyNameConstants.INVALID_LIMIT_EXCEPTION);
        }
        if (offset != null && offset < 0) {
            throw new TexeraException(PropertyNameConstants.INVALID_OFFSET_EXCEPTION);
        }

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
    public TupleSink newOperator() {
        return new TupleSink(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "View Results")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the results of the workflow")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
            .build();
    }

}
