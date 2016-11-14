package edu.uci.ics.textdb.common.field;

import edu.uci.ics.textdb.api.common.IField;

public class IDField implements IField {
    
    private final String _id;
    
    public IDField(String idValue) {
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

}
