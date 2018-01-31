package edu.uci.ics.texera.dataflow.sampler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 */
public class SamplerPredicate extends PredicateBase {
    
    public enum SampleType {
        RANDOM_SAMPLE("random"),

        FIRST_K_ARRIVAL("firstk");
        
        private final String name;
        
        private SampleType(String name) {
            this.name = name;
        }
        
        // use the name string instead of enum string in JSON
        @JsonValue
        public String getName() {
            return this.name;
        }
    }
    
    private final int sampleSize;
    private final SampleType sampleType;
    @JsonCreator
    public SamplerPredicate(
            @JsonProperty(value = PropertyNameConstants.SAMPLE_SIZE, required = true)
            Integer sampleSize,
            @JsonProperty(value = PropertyNameConstants.SAMPLE_TYPE, required = true)
            SampleType sampleType ) {
        if (sampleSize < 1) {
            throw new TexeraException("Sample size should be greater then 0.");
        }
        
        this.sampleSize = sampleSize;
        this.sampleType = sampleType;
    }
    
    @JsonProperty(PropertyNameConstants.SAMPLE_SIZE)
    public Integer getSampleSize() {
        return sampleSize;
    }
    
    @JsonProperty(PropertyNameConstants.SAMPLE_TYPE)
    public SampleType getSampleType() {
        return sampleType;
    }
    
    @Override
    public IOperator newOperator() {
        return new Sampler(this);
    }
    
}