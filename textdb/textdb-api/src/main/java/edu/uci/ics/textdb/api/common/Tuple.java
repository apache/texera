package edu.uci.ics.textdb.api.common;

import java.util.Arrays;
import java.util.List;

/**
 * @author chenli
 * @author sandeepreddy602
 * @author zuozhi
 * 
 * Created on 3/25/16.
 */
public class Tuple {
    private final Schema schema;
    private final List<IField> fields;

    public Tuple(Schema schema, IField... fields) {
        this.schema = schema;
        // Converting to java.util.Arrays.ArrayList
        // so that the collection remains static and cannot be extended/shrunk
        // This makes List<IField> partially immutable.
        // Partial because we can still replace an element at particular index.
        this.fields = Arrays.asList(fields);
    }

    @SuppressWarnings("unchecked")
    public <T extends IField> T getField(int index) {
        return (T) fields.get(index);
    }
    
    public <T extends IField> T getField(int index, Class<T> fieldClass) {
        return getField(index);
    }

    public <T extends IField> T getField(String fieldName) {
        return getField(schema.getIndex(fieldName));
    }
    
    public <T extends IField> T getField(String fieldName, Class<T> fieldClass) {
        return getField(schema.getIndex(fieldName));
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tuple other = (Tuple) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        return true;
    }

    public String toString() {
        return "DataTuple [schema=" + schema + ", fields=" + fields + "]";
    }

    public List<IField> getFields() {
        return fields;
    }

    public Schema getSchema() {
        return schema;
    }
}
