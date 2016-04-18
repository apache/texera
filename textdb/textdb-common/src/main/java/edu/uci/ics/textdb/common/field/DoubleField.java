package edu.uci.ics.textdb.common.field;

import edu.uci.ics.textdb.api.common.IField;

public class DoubleField implements IField{
    
    private Double value;

    public DoubleField(Double value){
        this.value = value;
    }
    
    @Override
    public Double getValue() {
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
        DoubleField other = (DoubleField) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DoubleField [value=" + value + "]";
    }
}
