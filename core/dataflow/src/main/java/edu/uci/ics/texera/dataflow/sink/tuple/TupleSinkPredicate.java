package edu.uci.ics.texera.dataflow.sink.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
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
            @JsonProperty(value = PropertyNameConstants.LIMIT, required = false)
            Integer limit,
            @JsonProperty(value = PropertyNameConstants.OFFSET, required = false)
            Integer offset
            ) {
        
        if (limit != null && limit < 0) {
            throw new TexeraException("limit must be greater than 0");
        }
        if (offset != null && offset < 0) {
            throw new TexeraException("offset must be greater than 0");
        }
        
        this.limit = limit;
        if (this.limit == null) {
            this.limit = Integer.MAX_VALUE;
        }
        if (this.limit <= 0) {
            throw new TexeraException("limit must > 0");
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
    public IOperator newOperator() {
        return new TupleSink(this);
    }

}
