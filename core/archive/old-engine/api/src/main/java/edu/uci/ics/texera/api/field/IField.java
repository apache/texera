package edu.uci.ics.texera.api.field;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;

/**
 * A field is a cell in a table that contains the actual data.
 * 
 * Created by chenli on 3/31/16.
 */
public interface IField {
    
    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    Object getValue();
}
