package edu.uci.ics.texera.dataflow.nlp.sentiment;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;

public class NlpSentimentTest {

    /*
     * Test sentiment with a positive sentence, result should be 3 (positive)
     */
    @Test
    public void test1() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.POSITIVE);
    }

    /*
     * Test sentiment with a neutral sentence, result should be 2 (neutral)
     */
    @Test
    public void test2() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEUTRAL_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEUTRAL);
    }

    /*
     * Test sentiment with a negative sentence, result should be 1 (negative)
     */
    @Test
    public void test3() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
    }

    /*
     * Test sentiment with multiple sentences in a tweet, the sentiment result
     * should equal to the sentiment of the longest sentence. For this input
     * "Blabla. Bugs are always annoying. But programming is so super awesome. Blabla."
     * , the result should be 3 (positive)
     */
    @Test
    public void test4() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(NlpSentimentTestConstants.MULTIPLE_SENTENCES_TUPLE),
                NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.POSITIVE);
    }
}
