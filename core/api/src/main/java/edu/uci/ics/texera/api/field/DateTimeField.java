package edu.uci.ics.texera.api.field;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.constants.JsonConstants;
import org.mockito.internal.matchers.Null;

public class DateTimeField implements IField {
    
    private LocalDateTime localDateTime;

    public DateTimeField(LocalDateTime localDateTime) {
        this.localDateTime = localDateTime;
    }

    @JsonCreator
    public DateTimeField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE)
            String localDateTimeString) {
        if (localDateTimeString != null) {
            this.localDateTime = LocalDateTime.parse(localDateTimeString);
        } else {
            this.localDateTime = null;
        }
    }

    @JsonCreator
    public DateTimeField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE, required = true)
            String localDateTimeString,
            DateTimeFormatter formatter) {
        if (localDateTimeString != null) {
            this.localDateTime = LocalDateTime.parse(localDateTimeString,formatter);
        } else {
            this.localDateTime = null;
        }
    }

    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    public String getDateTimeString() {
        if (localDateTime == null) {
            return "";
        }
        return this.localDateTime.toString();
    }

    @JsonIgnore
    @Override
    public LocalDateTime getValue() {
        return this.localDateTime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((localDateTime == null) ? 0 : localDateTime.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DateTimeField other = (DateTimeField) obj;
        if (localDateTime == null) {
            if (other.localDateTime != null)
                return false;
        } else if (!localDateTime.equals(other.localDateTime))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DateTimeField [value=" + String.valueOf(localDateTime) + "]";
    }

}
