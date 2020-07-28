package edu.uci.ics.texera.dataflow.sink.linechart;


import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSink;
import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSinkTestConstants;
import edu.uci.ics.texera.dataflow.sink.visualization.LineChartEnum;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import org.junit.Test;



import static org.junit.Assert.*;

public class LineChartSinkTest {
    @Test
    public void test1() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( LineChartSinkTestConstants.getTuples(), LineChartSinkTestConstants.BAR_SCHEMA);
        LineChartSink lineChartSink = new LineChartSink(new LineChartSinkPredicate("name", Arrays.asList("grade"), LineChartEnum.LINE));
        lineChartSink.setInputOperator(tupleSource);
        lineChartSink.open();
        List<Tuple> resultTuples = lineChartSink.collectAllTuples();

        assertTrue(TestUtils.equals(LineChartSinkTestConstants.getResultTuples(), resultTuples));
    }

    @Test(expected = TexeraException.class)
    public void test2() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( LineChartSinkTestConstants.getTuples(), LineChartSinkTestConstants.BAR_SCHEMA);
        LineChartSink lineChartSink = new LineChartSink(new LineChartSinkPredicate("name", Arrays.asList(""), LineChartEnum.LINE));
        lineChartSink.setInputOperator(tupleSource);
        lineChartSink.open();
        lineChartSink.collectAllTuples();

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