package edu.uci.ics.texera.dataflow.udf;

import java.util.Arrays;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import junit.framework.Assert;

public class TestCase {
    
    private static IOperator inputOperator;
    
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
    
    private static Schema inputSchema = testTuple1.getSchema();
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        inputOperator = new TupleSourceOperator(Arrays.asList(testTuple1, testTuple2, testTuple3), inputSchema);
        
        UserDFOperator userDFOperator = new UserDFOperator(new UserDFOperatorPredicate("udf_operator_user.py"));
        userDFOperator.setInputOperator(inputOperator);
        
        userDFOperator.open();
        Tuple tuple = null;
        int i = 0;
        while ((tuple = userDFOperator.getNextTuple()) != null) {
            System.out.println(i++);
            System.out.println(tuple.toString());
        }
        userDFOperator.close();
    }
    
}
