
package edu.uci.ics.textdb.api.field;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;

public class ListField<T> implements IField {

    private List<T> list;

    @JsonCreator
    public ListField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE, required = true)
            List<T> list) {
        // TODO: make a copy of the list to avoid modifying the list
        // but need to investigate the cost of doing so
        this.list = list;
    }

    @Override
    public List<T> getValue() {
        return new ArrayList<>(list);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ListField<?>)) {
            return false;
        }

        ListField<?> other = (ListField<?>) obj;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!(list.containsAll(other.list) & other.list.containsAll(list)))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ListField [value=" + list.toString() + "]";
    }
}
