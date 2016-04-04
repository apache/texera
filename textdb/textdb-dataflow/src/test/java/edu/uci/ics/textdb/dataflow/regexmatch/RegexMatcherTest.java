package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.dataflow.common.SampleRegexPredicate;
import edu.uci.ics.textdb.dataflow.source.SampleSourceOperator;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {

    @Test
    public void testSamplePipeline() throws Exception {
        IPredicate predicate = new SampleRegexPredicate("f*", SampleSourceOperator.FIRST_NAME);

        RegexMatcher matcher = new RegexMatcher(predicate, new SampleSourceOperator());
        for (int i = 0; i < SampleSourceOperator.SAMPLE_TUPLES.size(); i++) {
            Assert.assertEquals(matcher.getNextTuple(), SampleSourceOperator.SAMPLE_TUPLES.get(i));
        }
    }

}