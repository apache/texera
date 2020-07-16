package edu.uci.ics.texera.api.field;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;

public class IDField implements IField {
    
    private final String _id;
    
    @JsonCreator
    public IDField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE)
            String idValue) {
        checkNotNull(idValue);
        this._id = idValue;
    }

    @Override
    public String getValue() {
        return this._id;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IDField that = (IDField) o;
        return _id != null ? _id.equals(that._id) : that._id == null;
    }

    @Override
    public int hashCode() {
        return _id != null ? _id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "IDField [_id = " + _id + "]";
    }
    
    /**
     * Generates a new IDField with a random UUID.
     * 
     * @return
     */
    public static IDField newRandomID() {
        return new IDField(UUID.randomUUID().toString());
    }

}
