/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author avinash
 *
 */
public enum AggregationType {
    MIN,
    MAX,
    AVERAGE,
    SUM,
    COUNT
}