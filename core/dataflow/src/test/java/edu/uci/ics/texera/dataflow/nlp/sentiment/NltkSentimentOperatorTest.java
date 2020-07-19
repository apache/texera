package edu.uci.ics.texera.dataflow.nlp.sentiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;

public class NltkSentimentOperatorTest {
    private static String MODEL_FILE_NAME = "NltkSentiment.pickle";
    private static int BATCH_SIZE = 1000;
    
    /*
     * Test sentiment test result should be positive.
     */
    @Test
    public void test1() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NltkSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator nltkSentimentOperator = new NltkSentimentOperator(new NltkSentimentOperatorPredicate(
                NlpSentimentTestConstants.TEXT, "sentiment", BATCH_SIZE, MODEL_FILE_NAME));
        TupleSink tupleSink = new TupleSink();
        
        nltkSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(nltkSentimentOperator);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.POSITIVE);
    }
    
    /*
     * Test sentiment test result should be negative
     */
    @Test
    public void test2() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NltkSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator nltkSentimentOperator = new NltkSentimentOperator(new NltkSentimentOperatorPredicate(
                NltkSentimentTestConstants.TEXT, "sentiment", BATCH_SIZE, MODEL_FILE_NAME));
        
        TupleSink tupleSink = new TupleSink();
        
        nltkSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(nltkSentimentOperator);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
    }
    
    /*
     * Test batch processing of operator. All test results should be negative
     */
    @Test
    public void test3() throws TexeraException {
        int batchSize = 30;
        int tupleSourceSize = 101;
        
        List<Tuple> listTuple = new ArrayList<>();
        for (int i = 0; i < tupleSourceSize; i++) {
            listTuple.add(NltkSentimentTestConstants.NEGATIVE_TUPLE);
        }
        TupleSourceOperator tupleSource = new TupleSourceOperator(listTuple,
                NltkSentimentTestConstants.SENTIMENT_SCHEMA);
        NltkSentimentOperator nltkSentimentOperator = new NltkSentimentOperator(new NltkSentimentOperatorPredicate(
                NlpSentimentTestConstants.TEXT, "sentiment", batchSize, MODEL_FILE_NAME));
        
        TupleSink tupleSink = new TupleSink();
        
        nltkSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(nltkSentimentOperator);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        for (int i = 0; i < tupleSourceSize; i++) {
            Tuple tuple = results.get(i);
            Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
        }
    }
    
}
