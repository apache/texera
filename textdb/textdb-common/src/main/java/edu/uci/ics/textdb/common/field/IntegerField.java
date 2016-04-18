package edu.uci.ics.textdb.common.field;

import edu.uci.ics.textdb.api.common.IField;

public class IntegerField implements IField{
    
    private Integer value;
    
    public IntegerField(Integer value){
        this.value = value;
    }
    
    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        IntegerField other = (IntegerField) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IntegerField [value=" + value + "]";
    }
}
