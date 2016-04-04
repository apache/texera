package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chenli on 3/31/16.
 */
public class SampleTuple implements ITuple {
    private final List<String> schema;

    private final List<IField> fields;

    public SampleTuple(List<String> schema, IField... fields) {
        this.schema = schema;
        this.fields = Arrays.asList(fields);
    }

    @Override
    public IField getField(int index) {
        return fields.get(index);
    }

    @Override
    public IField getField(String fieldName) {
        int index = schema.indexOf(fieldName);
        if (index < 0) {
            return null;
        }
        return getField(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SampleTuple that = (SampleTuple) o;

        if (schema != that.schema) return false;
        return fields.equals(that.fields);

    }
}

