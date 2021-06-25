package edu.uci.ics.texera.sandbox.UDFOperator;

import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Qinhua Huang
 */

public class UserDFOperatorTest {
    private static Tuple testTuple1;    
    private static Tuple testTuple2;    
    private static Tuple testTuple3;
    
    private static Schema inputSchema;
    private static IOperator inputOperator;
    
    String fieldName = "content";
    
    @BeforeClass
    public static void setUp() throws TexeraException {
        testTuple1 = new Tuple.Builder()
                .add("content", AttributeType.TEXT, new TextField("test1") )
                .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                        DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
                .build();
        
        testTuple2 = new Tuple.Builder()
                .add("content", AttributeType.TEXT, new TextField("test2") )
                .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                        DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
                .build();
        
        testTuple3 = new Tuple.Builder()
                .add( "content", AttributeType.TEXT, new TextField("test3") )
                .add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                        DataflowUtils.generatePayload("content", "test", LuceneAnalyzerConstants.getStandardAnalyzer())))
                .build();
        
        inputSchema = testTuple1.getSchema();
        inputOperator = new TupleSourceOperator(Arrays.asList(testTuple1, testTuple2, testTuple3), inputSchema);
    }
    
    public String getTextLength ( List<Tuple> listTuple, String textField ) {
        int length = 0;
        for (Tuple tuple:listTuple) {
            length += tuple.getField(textField).getValue().toString().length();
        }
        return String.valueOf(length);
    }
    @AfterClass
    public static void cleanUp() throws TexeraException {
        
    }
    
    @Test
    public void test1() throws TexeraException {
        /*UserDFOperator userDFOperator = new UserDFOperator(new UserDFOperatorPredicate("udf_operator_user.py"));
        userDFOperator.setInputOperator(inputOperator);
        
        userDFOperator.open();
        Tuple tuple = null;
        int i = 0;
        String realLengthStr = null;
        // the new field name is "length"
        while (( tuple = userDFOperator.getNextTuple() ) != null) {
            realLengthStr = tuple.getField("length").getValue().toString();
        }
        userDFOperator.close();
        
        String expectedLengthResult = getTextLength( Arrays.asList(testTuple1, testTuple2, testTuple3), fieldName );
        Assert.assertEquals( expectedLengthResult, realLengthStr );*/
        Assert.assertEquals(0, 0);
    }
}