
package edu.uci.ics.textdb.common.field;

import java.util.List;

import edu.uci.ics.textdb.api.common.IField;

public class ListField<T> implements IField {

    private List<T> list;

    public ListField(List<T> list) {
        this.list = list;
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
        if (getClass() != obj.getClass())
            return false;
        ListField other = (ListField) obj;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if ( !(list.containsAll(other.list) & other.list.containsAll(list)))
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
