/**
 * 
 */
package edu.uci.ics.textdb.common.field;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;

/**
 * @author sandeepreddy602
 *
 */
public class DataTuple implements ITuple{
    private final List<Attribute> schema;
    private final List<IField> fields;

    public DataTuple(List<Attribute> schema, IField... fields) {
        this.schema = schema;
        this.fields = Arrays.asList(fields);
    }
    
    public IField getField(int index) {
        return fields.get(index);
    }

    public IField getField(String fieldName) {
        int index = -1;
        for (int count = 0; count < schema.size(); count++) {
            Attribute attr = schema.get(count);
            if(attr.getFieldName().equalsIgnoreCase(fieldName)){
                index = count;
                break;
            }
        }
        if (index < 0) {
            return null;
        }
        return getField(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataTuple that = (DataTuple) o;

        if (schema != that.schema) return false;
        return fields.equals(that.fields);

    }

    @Override
    public String toString() {
        return "DataTuple [schema=" + schema + ", fields=" + fields + "]";
    }

    public List<IField> getFields() {
        return fields;
    }
    
    public List<Attribute> getSchema() {
        return schema;
    }
}
