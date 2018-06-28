package edu.uci.ics.texera.dataflow.aggregator;

public class AggregatorErrorMessages {

    public static final String ATTRIBUTE_TYPE_NOT_FIT_FOR_AGGREGATION(String attributeName, String aggregationType) {
        return String.format("Attribute %s is not fit for aggregation of type %s", attributeName, aggregationType);
    }
}
