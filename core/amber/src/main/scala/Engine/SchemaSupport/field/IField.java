package Engine.SchemaSupport.field;

import com.fasterxml.jackson.annotation.JsonProperty;
import Engine.SchemaSupport.constants.JsonConstants;

import java.io.Serializable;

/**
 * A field is a cell in a table that contains the actual data.
 * 
 * Created by chenli on 3/31/16.
 */
public interface IField extends Serializable {
    
    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    Object getValue();
}
