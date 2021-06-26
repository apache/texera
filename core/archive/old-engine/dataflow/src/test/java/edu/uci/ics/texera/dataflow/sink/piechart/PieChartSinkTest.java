package edu.uci.ics.texera.dataflow.sink.piechart;

import static org.junit.Assert.*;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.visualization.PieChartEnum;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import java.util.List;
import org.junit.Test;

public class PieChartSinkTest {

    @Test
    public void test1() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( PieChartSinkTestConstants.getTuples(), PieChartSinkTestConstants.PIE_SCHEMA);
        PieChartSink pieChartSink = new PieChartSink(new PieChartSinkPredicate("name", "number_of_followers", 0.9, PieChartEnum.PIE));
        pieChartSink.setInputOperator(tupleSource);
        pieChartSink.open();
        List<Tuple> resultTuples = pieChartSink.collectAllTuples();
        assertEquals(2, resultTuples.size());
    }

    @Test(expected = TexeraException.class)
    public void test2() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( PieChartSinkTestConstants.getTuples(), PieChartSinkTestConstants.PIE_SCHEMA);
        PieChartSink pieChartSink = new PieChartSink(new PieChartSinkPredicate("name", "number_of_followers", -0.9, PieChartEnum.PIE));
        pieChartSink.setInputOperator(tupleSource);
        pieChartSink.open();
        pieChartSink.collectAllTuples();

    }

    @Test(expected = TexeraException.class)
    public void test3() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( PieChartSinkTestConstants.getTuples(), PieChartSinkTestConstants.PIE_SCHEMA);
        PieChartSink pieChartSink = new PieChartSink(new PieChartSinkPredicate("name", "gender", 0.9, PieChartEnum.PIE));
        pieChartSink.setInputOperator(tupleSource);
        pieChartSink.open();
        pieChartSink.collectAllTuples();
    }

    @Test(expected = TexeraException.class)
    public void test4() {
        TupleSourceOperator tupleSource = new TupleSourceOperator( PieChartSinkTestConstants.getTuples(), PieChartSinkTestConstants.PIE_SCHEMA);
        PieChartSink pieChartSink = new PieChartSink(new PieChartSinkPredicate("name", "aaaaa", 0.9, PieChartEnum.PIE));
        pieChartSink.setInputOperator(tupleSource);
        pieChartSink.open();
        pieChartSink.collectAllTuples();
    }

}