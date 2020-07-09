package edu.uci.ics.texera.dataflow.sink.barchart;

import static org.junit.Assert.*;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class BarChartSinkTest {

    @Test
    public void test1() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( BarChartSinkTestConstants.getTuples(), BarChartSinkTestConstants.BAR_SCHEMA);
        BarChartSink barChartSink = new BarChartSink(new BarChartSinkPredicate("name", Arrays.asList("grade")));
        barChartSink.setInputOperator(tupleSource);
        barChartSink.open();
        List<Tuple> resultTuples = barChartSink.collectAllTuples();

        assertTrue(TestUtils.equals(BarChartSinkTestConstants.getResultTuples(), resultTuples));
    }

    @Test(expected = TexeraException.class)
    public void test2() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( BarChartSinkTestConstants.getTuples(), BarChartSinkTestConstants.BAR_SCHEMA);
        BarChartSink barChartSink = new BarChartSink(new BarChartSinkPredicate("name", Arrays.asList("")));
        barChartSink.setInputOperator(tupleSource);
        barChartSink.open();
        barChartSink.collectAllTuples();
    }

    @Test(expected = TexeraException.class)
    public void test3() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( BarChartSinkTestConstants.getTuples(), BarChartSinkTestConstants.BAR_SCHEMA);
        BarChartSink barChartSink = new BarChartSink(new BarChartSinkPredicate("name", Arrays.asList("gender")));
        barChartSink.setInputOperator(tupleSource);
        barChartSink.open();
        barChartSink.collectAllTuples();
    }
}