package edu.uci.ics.texera.dataflow.sink.tuple;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import junit.framework.Assert;

public class TupleSinkTest {
    
    private IOperator inputOperator;
    
    private static Tuple testTuple1 = new Tuple.Builder()
            .add("content", AttributeType.TEXT, new TextField("test1"))
            .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                    DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
            .build();
    
    private static Tuple testTuple2 = new Tuple.Builder()
            .add("content", AttributeType.TEXT, new TextField("test2"))
            .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                    DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
            .build();
    
    private static Tuple testTuple3 = new Tuple.Builder()
            .add("content", AttributeType.TEXT, new TextField("test3"))
            .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                    DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
            .build();
    
    private Schema inputSchema = testTuple1.getSchema();
            
    
    @Before
    public void setUp() throws FileNotFoundException {
        inputOperator = new TupleSourceOperator(Arrays.asList(testTuple1, testTuple2, testTuple3), inputSchema);
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
        // assert that the tuple stream sink removes the PAYLOAD attribute
        Assert.assertEquals(
                new Schema(SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.TEXT)), 
                tupleSink.getOutputSchema());
        
        tupleSink.close();
    }

    @Test
    public void testGetNextTuple() throws Exception {
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(inputOperator);
        
        tupleSink.open();
        Tuple tuple = tupleSink.getNextTuple();
        
        Assert.assertEquals("test1", tuple.getField("content").getValue());

        tupleSink.close();
    }
    
    /*
     * Test tuple sink predicate with limit 1 and offset 1.
     */
    @Test
    public void testLimitOffset() throws Exception {
        TupleSink tupleSink = new TupleSink(new TupleSinkPredicate(1, 1));
        tupleSink.setInputOperator(inputOperator);
        
        tupleSink.open();
        Tuple resultTuple1 = tupleSink.getNextTuple();
        Tuple resultTuple2 = tupleSink.getNextTuple();
        tupleSink.close();
        
        Assert.assertEquals("test2", resultTuple1.getField("content").getValue());
        Assert.assertTrue(resultTuple2 == null);
    }
    
}
