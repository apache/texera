package edu.uci.ics.texera.dataflow.nlp.splitter;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;




public class NlpSplitTest {
    
    @Test
    public void test1() throws TexeraException, ParseException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                NlpSplitTestConstants.getOneToOneTestTuple(), NlpSplitTestConstants.SPLIT_SCHEMA);
        NlpSplitOperator sentence_list = new NlpSplitOperator(
                new NlpSplitPredicate(NLPOutputType.ONE_TO_ONE, NlpSplitTestConstants.TEXT, SchemaConstants.SPAN_LIST));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        Assert.assertTrue(TestUtils.equals(NlpSplitTestConstants.getOneToOneResultTuple(), results));
    }
    
    @Test
    public void test2() throws TexeraException, ParseException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                NlpSplitTestConstants.getOneToManyTestTuple(), NlpSplitTestConstants.SPLIT_SCHEMA);
        NlpSplitOperator sentence_list = new NlpSplitOperator(
                new NlpSplitPredicate(NLPOutputType.ONE_TO_MANY, NlpSplitTestConstants.TEXT, PropertyNameConstants.NLP_OUTPUT_TYPE));
        TupleSink tupleSink = new TupleSink();
        
        sentence_list.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentence_list);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        Assert.assertTrue(TestUtils.equals(NlpSplitTestConstants.getOneToManyResultTuple(), results));
        Set<IDField> compset = new HashSet<IDField>();
        for(Tuple result : results) {
            Assert.assertFalse(compset.contains(result.getField(SchemaConstants._ID)));
            compset.add(result.getField(SchemaConstants._ID));
        }
        
    }
}
