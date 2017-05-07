package edu.uci.ics.textdb.exp.sink.tuple;

import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import junit.framework.Assert;

public class TupleSinkTest {
    
    private IOperator inputOperator;
    private Schema inputSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT), SchemaConstants.PAYLOAD_ATTRIBUTE);

    @Before
    public void setUp() throws FileNotFoundException {
        inputOperator = Mockito.mock(IOperator.class);
        Mockito.when(inputOperator.getOutputSchema()).thenReturn(inputSchema);
    }

    @After
    public void cleanUp() {
    }

    @Test
    public void testOpenClose() throws Exception {
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(inputOperator);
        
        tupleSink.open();
        // verify that inputOperator called open() method
        Mockito.verify(inputOperator).open();
        // assert that the tuple stream sink removes the PAYLOAD attribute
        Assert.assertEquals(
                new Schema(SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT)), 
                tupleSink.getOutputSchema());
        
        tupleSink.close();
        // verify that inputOperator called close() method
        Mockito.verify(inputOperator).close();
    }

    @Test
    public void testGetNextTuple() throws Exception {
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(inputOperator);
        
        Tuple sampleTuple = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
        Mockito.when(sampleTuple.getSchema()).thenReturn(inputSchema);
        // Set the behavior for inputOperator,
        // first it returns some non-null tuple and second time it returns null
        Mockito.when(inputOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
        
        tupleSink.open();
        tupleSink.getNextTuple();
        
        // Verify that input operator's getNextTuple is called
        Mockito.verify(inputOperator, Mockito.times(1)).getNextTuple();

        tupleSink.close();
    }
    
    /*
     * Test tuple sink predicate with limit 1 and offset 1.
     */
    @Test
    public void testLimitOffset() throws Exception {
        TupleSink tupleSink = new TupleSink(new TupleSinkPredicate(1, 1));
        tupleSink.setInputOperator(inputOperator);
        
        Tuple sampleTuple1 = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple1.getSchema()).thenReturn(inputSchema);
        
        Tuple sampleTuple2 = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple2.getSchema()).thenReturn(inputSchema);

        Tuple sampleTuple3 = Mockito.mock(Tuple.class);
        Mockito.when(sampleTuple3.getSchema()).thenReturn(inputSchema);

        // Set the behavior for inputOperator,
        // it returns 3 tuples, then return null
        Mockito.when(inputOperator.getNextTuple()).thenReturn(sampleTuple1)
            .thenReturn(sampleTuple2).thenReturn(sampleTuple3).thenReturn(null);
        
        tupleSink.open();
        Tuple resultTuple1 = tupleSink.getNextTuple();
        Tuple resultTuple2 = tupleSink.getNextTuple();
        tupleSink.close();
        
        Assert.assertTrue(resultTuple1 != null);
        Assert.assertTrue(resultTuple2 == null);
    }
    
}
