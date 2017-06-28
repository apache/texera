package edu.uci.ics.textdb.exp.nltk;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentTestConstants;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;

public class NltkTest {
    /*
     * Test sentiment with a positive sentence, result should be 3 (negative)
     */
    @Test
    public void test1() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        Nltk sentiment = new Nltk(new NltkPredicate(NlpSentimentTestConstants.TEXT, "sentiment")
                );
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), "neg");
    }
    
    /*
     * Test sentiment with a neutral sentence, result should be 2 (positive)
     */
    @Test
    public void test2() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEUTRAL_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        Nltk sentiment = new Nltk(new NltkPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), "pos");     
    }
    
    /*
     * Test sentiment with a negative sentence, result should be 1 (negative)
     */
    @Test
    public void test3() throws TextDBException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        Nltk sentiment = new Nltk(
                new NltkPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        
        TupleSink tupleSink = new TupleSink();
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), "neg");        
    }

}
