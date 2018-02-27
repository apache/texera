package edu.uci.ics.texera.api.field;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;

public class DateField implements IField {
    
    private LocalDate localDate;


    public DateField(Date date) {
        checkNotNull(date);
        this.localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public DateField(LocalDate localDate) {
        checkNotNull(localDate);
        this.localDate = localDate;
    }

    @JsonCreator
    public DateField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE, required = true) 
            String localDateString) {
        checkNotNull(localDateString);
        this.localDate = LocalDate.parse(localDateString);
    }

    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    public String getDateString() {
        return this.localDate.toString();
    }

    @JsonIgnore
    @Override
    public LocalDate getValue() {
        return this.localDate;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((localDate == null) ? 0 : localDate.hashCode());
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
        DateField other = (DateField) obj;
        if (localDate == null) {
            if (other.localDate != null)
                return false;
        } else if (!localDate.equals(other.localDate))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DateField [value=" + localDate.toString() + "]";
    }

}
