package edu.uci.ics.textdb.dataflow.regexmatch;

import org.junit.Ignore;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.dataflow.common.SampleRegexPredicate;
import edu.uci.ics.textdb.dataflow.source.Constants;
import edu.uci.ics.textdb.dataflow.source.SampleDataStoreTest;
import edu.uci.ics.textdb.dataflow.source.SampleSourceOperator;
import junit.framework.Assert;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {

	/**
	 * Ignoring this test so that travis CI build doesn't fail.
	 * We need to have Lucene index created to run this test case.
	 * run {@link SampleDataStoreTest} testStoreData method to populate Lucene Index
	 * and then remove @Ignore annotation to run this test case.
	 * @throws Exception
	 */
    @Test
    @Ignore
    public void testSamplePipeline() throws Exception {
        IPredicate predicate = new SampleRegexPredicate("f.", Constants.FIRST_NAME);

        RegexMatcher matcher = new RegexMatcher(predicate, new SampleSourceOperator());
        for (int i = 0; i < Constants.SAMPLE_TUPLES.size(); i++) {

        	ITuple actualTuple = matcher.getNextTuple();
        	ITuple expectedTuple = Constants.SAMPLE_TUPLES.get(i);
            assertEquality(actualTuple, expectedTuple);
        	System.out.println("Actual Tuple: "+actualTuple);
        	System.out.println("Expected Tuple: "+Constants.SAMPLE_TUPLES.get(i));
        	System.out.println();
        	
        }
    }

	private void assertEquality(ITuple expectedTuple, ITuple actualTuple) {
		int schemaSize = Constants.SAMPLE_SCHEMA.size();
		for (int i = 0; i < schemaSize; i++) {
			Assert.assertEquals(expectedTuple.getField(i), actualTuple.getField(i));
		}
		
	}

}