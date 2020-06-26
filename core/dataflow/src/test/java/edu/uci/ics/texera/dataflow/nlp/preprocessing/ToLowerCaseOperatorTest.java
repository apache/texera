package edu.uci.ics.texera.dataflow.nlp.preprocessing;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.nlp.splitter.NlpSplitTestConstants;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import java.text.ParseException;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ToLowerCaseOperatorTest {
    @Test
    public void test1() throws TexeraException, ParseException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
            ToLowerCaseOperatorTestConstants.getTestTuple(), ToLowerCaseOperatorTestConstants.SCHMEA);
        ToLowerCaseOperator tolowerCaseOperator = new ToLowerCaseOperator(
            new ToLowerCasePredicate("tweet", "result"));
        TupleSink tupleSink = new TupleSink();

        tolowerCaseOperator.setInputOperator(tupleSource);
        tupleSink.setInputOperator(tolowerCaseOperator);

        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        tupleSink.close();
        Assert.assertTrue(TestUtils.equals(NlpSplitTestConstants.getOneToOneResultTuple(), results));
    }

}
