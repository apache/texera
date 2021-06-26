
package edu.uci.ics.texera.api.field;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;

import edu.uci.ics.texera.api.constants.JsonConstants;

@JsonDeserialize(using = ListFieldJsonDeserializer.class)
public class ListField<T> implements IField {

    private ImmutableList<T> list;

    @JsonCreator
    public ListField(
            @JsonProperty(value = JsonConstants.FIELD_VALUE)
            List<T> list) {
        this.list = ImmutableList.copyOf(list);
    }

    @Override
    public List<T> getValue() {
        return list;
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
        String getStringResult = new String();
        for (T val : list) {
            getStringResult = getStringResult.concat(val.toString().concat(" "));
        }
        getStringResult = getStringResult.trim();
        return "ListField [value=" + getStringResult + "]";
    }
}
