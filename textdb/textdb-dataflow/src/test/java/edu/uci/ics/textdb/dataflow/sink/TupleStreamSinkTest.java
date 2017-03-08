package edu.uci.ics.textdb.dataflow.sink;

import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import junit.framework.Assert;

public class TupleStreamSinkTest {
    
    private TupleStreamSink tupleStreamSink;
    private IOperator inputOperator;
    private Schema inputSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", FieldType.TEXT), SchemaConstants.PAYLOAD_ATTRIBUTE);

    @Before
    public void setUp() throws FileNotFoundException {
        inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(inputSchema);
        
        tupleStreamSink = new TupleStreamSink();
        tupleStreamSink.setInputOperator(inputOperator);
    }

    @After
    public void cleanUp() {
    }

    @Test
    public void testOpen() throws Exception {
        tupleStreamSink.open();
        // verify that inputOperator called open() method
        Mockito.verify(inputOperator).open();
        // assert that the tuple stream sink removes the _ID and PAYLOAD attribute
        Assert.assertEquals(new Schema(new Attribute("content", FieldType.TEXT)), tupleStreamSink.getOutputSchema());
    }

    @Test
    public void testClose() throws Exception {
        tupleStreamSink.close();
        // verify that inputOperator called close() method
        Mockito.verify(inputOperator).close();
    }

    @Test
    public void testGetNextTuple() throws Exception {
        Tuple sampleTuple = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
        Mockito.when(sampleTuple.getSchema()).thenReturn(inputSchema);
        // Set the behavior for inputOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(inputOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        
        tupleStreamSink.open();
        tupleStreamSink.getNextTuple();
        
        // Verify that input operator's getNextTuple is called
        Mockito.verify(inputOperator, Mockito.times(1)).getNextTuple();

        tupleStreamSink.close();
    }
    
}
