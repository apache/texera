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

public class MLSentimentOperatorTest {
    private static int BATCH_SIZE = 1000;

    /*
     * Test sentiment test result should be positive.
     */
    @Test
    public void positive_test() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(MLSentimentTestConstants.POSITIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        MLSentimentOperator mlSentimentOperator = new MLSentimentOperator(new MLSentimentOperatorPredicate(
                NlpSentimentTestConstants.TEXT, "sentiment", BATCH_SIZE));
        TupleSink tupleSink = new TupleSink();

        mlSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(mlSentimentOperator);

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
    public void negative_test() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(MLSentimentTestConstants.NEGATIVE_TUPLE), NlpSentimentTestConstants.SENTIMENT_SCHEMA);
        MLSentimentOperator mlSentimentOperator = new MLSentimentOperator(new MLSentimentOperatorPredicate(
                NlpSentimentTestConstants.TEXT, "sentiment", BATCH_SIZE));
        TupleSink tupleSink = new TupleSink();

        mlSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(mlSentimentOperator);

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
    public void group_negative_test() throws TexeraException {
        int batchSize = 30;
        int tupleSourceSize = 101;

        List<Tuple> listTuple = new ArrayList<>();
        for (int i = 0; i < tupleSourceSize; i++) {
            listTuple.add(MLSentimentTestConstants.NEGATIVE_TUPLE);
        }
        TupleSourceOperator tupleSource = new TupleSourceOperator(listTuple,
                MLSentimentTestConstants.SENTIMENT_SCHEMA);
        MLSentimentOperator mlSentimentOperator = new MLSentimentOperator(new MLSentimentOperatorPredicate(
                NlpSentimentTestConstants.TEXT, "sentiment", BATCH_SIZE));
        TupleSink tupleSink = new TupleSink();

        mlSentimentOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(mlSentimentOperator);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        for (int i = 0; i < tupleSourceSize; i++) {
            Tuple tuple = results.get(i);
            Assert.assertEquals(tuple.getField("sentiment").getValue(), SentimentConstants.NEGATIVE);
        }
    }

    public static void main(String[] args) {
        MLSentimentOperatorTest test = new MLSentimentOperatorTest();
        test.positive_test();
        test.negative_test();
        test.group_negative_test();
    }
}
