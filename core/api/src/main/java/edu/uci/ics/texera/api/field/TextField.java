package edu.uci.ics.texera.api.field;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;

/**
 * Created by chenli on 3/31/16. A field that is indexed and tokenized, without
 * term vectors. For example this would be used on a 'body' field, that contains
 * the bulk of a document's text.
 */
public class TextField implements IField {

    private final String value;

    @JsonCreator
    public TextField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE)
            String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TextField that = (TextField) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TextField [value=" + value + "]";
    }

}
