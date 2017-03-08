package edu.uci.ics.textdb.dataflow.sink;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

public class AbstractSinkTest {

    private AbstractSink sink;
    private IOperator childOperator;

    @Before
    public void setUp() {
        childOperator = Mockito.mock(IOperator.class);
        sink = new AbstractSink() {
            @Override
            protected void processOneTuple(Tuple nextTuple) {

            }
        };
        sink.setInputOperator(childOperator);
    }

    @Test
    public void testOpen() throws Exception {
        sink.open();
        // verify that childOperator called open() method
        Mockito.verify(childOperator).open();
    }

    @Test
    public void testClose() throws Exception {
        sink.close();
        // verify that childOperator called close() method
        Mockito.verify(childOperator).close();
    }

    @Test
    public void testProcessTuples() throws Exception {
        Tuple sampleTuple = Mockito.mock(Tuple.class);
        // Set the behavior for childOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(childOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        sink.processTuples();
        // Verify that childOperator.getNextTuple() is called twice
        Mockito.verify(childOperator, Mockito.times(2)).getNextTuple();
    }
}
