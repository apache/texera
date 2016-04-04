package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.SampleTuple;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chenli on 3/31/16.
 */
public class SampleSourceOperator implements ISourceOperator {
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";

    public static final List<String> SAMPLE_SCHEMA = Arrays.asList(FIRST_NAME, LAST_NAME);

    public static final List<ITuple> SAMPLE_TUPLES = Arrays.asList(
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f1"), new StringField("l1")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f2"), new StringField("l2")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f3"), new StringField("l3")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f4"), new StringField("l4")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f5"), new StringField("l5")));

    private int cursor;

    public SampleSourceOperator() {
    }

    @Override
    public void open() {
        cursor = 0;
    }

    @Override
    public ITuple getNextTuple() {
        if (cursor < 0 || cursor >= SAMPLE_TUPLES.size()) {
            return null;
        }
        return SAMPLE_TUPLES.get(cursor++);
    }

    @Override
    public void close() {
    }

}
