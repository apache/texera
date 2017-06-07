package edu.uci.ics.textdb.exp.nlp.sentiment;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Vinay on 23-05-2017.
 */
public class EmojiSentimentTest {
    @Test
    public void test1() throws TextDBException {
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
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 3);
    }

    /*
     * Test sentiment with a neutral sentence, result should be 2 (neutral)
     */
    @Test
    public void test2() throws TextDBException {
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
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 2);
    }

    /*
     * Test sentiment with a negative sentence, result should be 1 (negative)
     */

    @Test
    public void test3() throws TextDBException {
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
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 1);
    }
    @Test
    public void test4() throws TextDBException {
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
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 3);
    }
    @Test
    public void test5() throws TextDBException {
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
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 1);
    }
}
