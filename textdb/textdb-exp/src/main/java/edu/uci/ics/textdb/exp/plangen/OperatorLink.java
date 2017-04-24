package edu.uci.ics.textdb.exp.plangen;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class OperatorLink {
    
    private String origin;
    private String destination;
    
    /**
     * OperatorLink describes a link between two operators.
     * 
     * @param origin, the ID of the origin operator
     * @param destination, the ID of the destination operator
     */
    @JsonCreator
    public OperatorLink(
            @JsonProperty(value = PropertyNameConstants.ORIGIN_OPERATOR_ID, required = true)
            String origin,
            @JsonProperty(value = PropertyNameConstants.DESTINATION_OPERATOR_ID, required = true)
            String destination) {
        this.origin = origin;
        this.destination = destination;
    }
    
    @JsonProperty(value = PropertyNameConstants.ORIGIN_OPERATOR_ID)
    public String getOrigin() {
        return this.origin;
    }
    
    @JsonProperty(value = PropertyNameConstants.DESTINATION_OPERATOR_ID)
    public String getDestination() {
        return this.destination;
    }

}
