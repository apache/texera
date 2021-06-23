package edu.uci.ics.texera.dataflow.nlp.sentiment;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Vinay on 23-05-2017.
 */
public class EmojiSentimentTest {
    @Test
    public void test1() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(EmojiSentimentTestConstants.POSITIVE_TUPLE1), EmojiSentimentTestConstants.SENTIMENT_SCHEMA);
        EmojiSentimentOperator sentiment = new EmojiSentimentOperator(
                new EmojiSentimentPredicate(EmojiSentimentTestConstants.TEXT, "sentiment"));
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
                Arrays.asList(EmojiSentimentTestConstants.NEUTRAL_TUPLE), EmojiSentimentTestConstants.SENTIMENT_SCHEMA);
        EmojiSentimentOperator sentiment = new EmojiSentimentOperator(
                new EmojiSentimentPredicate(EmojiSentimentTestConstants.TEXT, "sentiment"));
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
                Arrays.asList(EmojiSentimentTestConstants.NEGATIVE_TUPLE1), EmojiSentimentTestConstants.SENTIMENT_SCHEMA);
        EmojiSentimentOperator sentiment = new EmojiSentimentOperator(
                new EmojiSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
    }
    @Test
    public void test4() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(EmojiSentimentTestConstants.POSITIVE_TUPLE2), EmojiSentimentTestConstants.SENTIMENT_SCHEMA);
        EmojiSentimentOperator sentiment = new EmojiSentimentOperator(
                new EmojiSentimentPredicate(EmojiSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.POSITIVE);
    }
    @Test
    public void test5() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(EmojiSentimentTestConstants.NEGATIVE_TUPLE2), EmojiSentimentTestConstants.SENTIMENT_SCHEMA);
        EmojiSentimentOperator sentiment = new EmojiSentimentOperator(
                new EmojiSentimentPredicate(NlpSentimentTestConstants.TEXT, "sentiment"));
        TupleSink tupleSink = new TupleSink();

        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();

        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
    }
}
