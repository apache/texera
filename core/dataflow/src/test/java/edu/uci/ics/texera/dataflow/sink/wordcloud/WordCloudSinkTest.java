package edu.uci.ics.texera.dataflow.sink.wordcloud;


import static org.junit.Assert.assertTrue;

import edu.uci.ics.texera.api.exception.TexeraException;
import java.util.List;

import edu.uci.ics.texera.dataflow.sink.wordcloud.WordCloudSink;
import edu.uci.ics.texera.dataflow.sink.wordcloud.WordCloudSinkPredicate;
import org.junit.Test;

import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSinkTestConstants;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;


public class WordCloudSinkTest {
    @Test
    public void test1() {
        TupleSourceOperator tupleSource = new TupleSourceOperator(WordCloudSinkTestConstants.getTuples(), WordCloudSinkTestConstants.WORD_CLOUD_SCHEMA);
        WordCloudSink wordCloudSink = new WordCloudSink(new WordCloudSinkPredicate(WordCloudSinkTestConstants.ATTRIBUTE_NAME_ONE, WordCloudSinkTestConstants.ANALYZER_TYPE));
        wordCloudSink.setInputOperator(tupleSource);
        wordCloudSink.open();
        List<Tuple> resultTuples = wordCloudSink.collectAllTuples();

        assertTrue(TestUtils.equals(WordCloudSinkTestConstants.getResultTuples(), resultTuples));
    }

    @Test(expected = TexeraException.class)
    public void test2() {
        TupleSourceOperator tupleSource = new TupleSourceOperator(WordCloudSinkTestConstants.getTuples(), WordCloudSinkTestConstants.WORD_CLOUD_SCHEMA);
        WordCloudSink wordCloudSink = new WordCloudSink(new WordCloudSinkPredicate("w", WordCloudSinkTestConstants.ANALYZER_TYPE));
        wordCloudSink.setInputOperator(tupleSource);
        wordCloudSink.open();
        List<Tuple> resultTuples = wordCloudSink.collectAllTuples();
        assertTrue(TestUtils.equals(WordCloudSinkTestConstants.getResultTuples(), resultTuples));
    }

    @Test(expected = TexeraException.class)
    public void test3() {
        TupleSourceOperator tupleSource = new TupleSourceOperator(WordCloudSinkTestConstants.getTuples(), WordCloudSinkTestConstants.WORD_CLOUD_SCHEMA);
        WordCloudSink wordCloudSink = new WordCloudSink(new WordCloudSinkPredicate("count", WordCloudSinkTestConstants.ANALYZER_TYPE));
        wordCloudSink.setInputOperator(tupleSource);
        wordCloudSink.open();
        List<Tuple> resultTuples = wordCloudSink.collectAllTuples();
        TestUtils.equals(BarChartSinkTestConstants.getResultTuples(), resultTuples);
    }


}