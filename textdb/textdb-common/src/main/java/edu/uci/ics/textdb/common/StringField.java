package edu.uci.ics.textdb.common;


import edu.uci.ics.textdb.api.common.IField;

/**
 * Created by chenli on 3/31/16.
 */
public class StringField implements IField {

    private final String value;

    public StringField(String value){
        this.value = value;
    }

    String getValue(){
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringField that = (StringField) o;

        return !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
