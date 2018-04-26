package edu.uci.ics.texera.dataflow.sampler;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
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
            throw new TexeraException(PropertyNameConstants.INVALID_SAMPLE_SIZE_EXCEPTION);
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
    public Sampler newOperator() {
        return new Sampler(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Sampling")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Sample a subset of data from all the documents")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.UTILITY_GROUP)
            .build();
    }
    
}
