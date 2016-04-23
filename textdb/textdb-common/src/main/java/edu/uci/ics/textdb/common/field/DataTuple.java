package edu.uci.ics.textdb.common.field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

/**
 * @author sandeepreddy602
 *
 */
public class DataTuple implements ITuple {
    private final Schema schema;
    private final List<IField> fields;

    public DataTuple(Schema schema, IField... fields) {
        this.schema = schema;
        this.fields = new ArrayList<IField>(Arrays.asList(fields));
    }

    @Override
    public IField getField(int index) {
        return fields.get(index);
    }

    @Override
    public IField getField(String fieldName) {
        int index = -1;
        List<Attribute> attributes = schema.getAttributes();
        for (int count = 0; count < attributes.size(); count++) {
            Attribute attr = attributes.get(count);
            if (attr.getFieldName().equalsIgnoreCase(fieldName)) {
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataTuple that = (DataTuple) o;

        if (schema != that.schema)
            return false;
        return fields.equals(that.fields);

    }

    @Override
    public String toString() {
        return "DataTuple [schema=" + schema + ", fields=" + fields + "]";
    }

    @Override
    public List<IField> getFields() {
        return fields;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

}
