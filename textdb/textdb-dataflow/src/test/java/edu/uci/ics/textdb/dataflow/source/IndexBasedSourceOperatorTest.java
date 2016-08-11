/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author akshaybetala
 *
 */
public class IndexBasedSourceOperatorTest {

	private IDataWriter dataWriter;
	private IndexBasedSourceOperator indexBasedSourceOperator;
	private IDataStore dataStore;
	private Analyzer luceneAnalyzer;
    private DataReaderPredicate dataReaderPredicate;


	@Before
	public void setUp() throws Exception {
		dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
		luceneAnalyzer = new StandardAnalyzer();
		dataWriter = new DataWriter(dataStore, luceneAnalyzer);
		dataWriter.clearData();
		dataWriter.writeData(TestConstants.getSamplePeopleTuples());

	}

	@After
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}
	
	public void constructIndexBasedSourceOperator(String query) throws ParseException{
	    String defaultField = TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName();
        QueryParser queryParser = new QueryParser(defaultField, luceneAnalyzer);
        Query queryObject = queryParser.parse(query);
        dataReaderPredicate = new DataReaderPredicate(queryObject, query,
        		dataStore, Arrays.asList(TestConstants.ATTRIBUTES_PEOPLE[0]), luceneAnalyzer);

        indexBasedSourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
	}

	public List<ITuple> getQueryResults(String query) throws DataFlowException, ParseException {
		constructIndexBasedSourceOperator(query);
		indexBasedSourceOperator.open();

		List<ITuple> results = new ArrayList<ITuple>();
		ITuple nextTuple = null;
		while ((nextTuple = indexBasedSourceOperator.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		indexBasedSourceOperator.close();
		return results;
	}

	/**
	 * Search in a text field with multiple tokens.
	 * @throws DataFlowException
	 * @throws ParseException
	 */
	@Test
	public void testTextSearchWithMultipleTokens() throws DataFlowException, ParseException {
		List<ITuple> results = getQueryResults(TestConstants.DESCRIPTION + ":Tall,Brown");
		int numTuples = results.size();
		Assert.assertEquals(3, numTuples);

		boolean check = TestUtils.checkResults(results,"Tall,Brown" , this.luceneAnalyzer,TestConstants.DESCRIPTION);
		Assert.assertTrue(check);
	}

	/**
	 * Search in a text field with a single token
	 * 
	 * @throws DataFlowException
	 * @throws ParseException
	 */
	@Test
	public void testTextSearchWithSingleToken() throws DataFlowException, ParseException {
		List<ITuple> results = getQueryResults(TestConstants.DESCRIPTION + ":angry");
		int numTuples = results.size();
		boolean check = TestUtils.checkResults(results,"angry" , this.luceneAnalyzer,TestConstants.DESCRIPTION);
		Assert.assertTrue(check);
		Assert.assertEquals(3, numTuples);
	}

	/**
	 * Test a query on the string field, with a substring as the query 
	 * 			Should return no result
	 * 
	 * @throws DataFlowException
	 * @throws ParseException
	 */
	@Test
	public void testStringSearchWithSubstring() throws DataFlowException, ParseException {
		List<ITuple> results = getQueryResults("lin");
		int numTuples = results.size();
		Assert.assertEquals(0, numTuples);
	}

	/**
	 * 
	 * Test a query which has multiple field
	 * 
	 * @throws DataFlowException
	 * @throws ParseException
	 */
	@Test
	public void testMultipleFields() throws DataFlowException, ParseException {
		List<ITuple> results = getQueryResults(
				TestConstants.DESCRIPTION + ":(Tall,Brown)" + " AND " + TestConstants.LAST_NAME + ":cruise");
		int numTuples = results.size();
		Assert.assertEquals(1, numTuples);

		for (ITuple tuple : results) {
			String descriptionValue = (String) tuple.getField(TestConstants.DESCRIPTION).getValue();
			String lastNameValue = (String) tuple.getField(TestConstants.LAST_NAME).getValue();
			Assert.assertTrue(descriptionValue.toLowerCase().contains("tall")
					|| descriptionValue.toLowerCase().contains("brown"));
			Assert.assertTrue(lastNameValue.toLowerCase().contains("cruise"));
		}
	}
	
	/**
	 * Tests the scenario where the predicate is reset and the 
	 * getNextTupkle() is called without opening the operator again.
	 * This throws an Exception
	 * @throws ParseException
	 * @throws DataFlowException
	 */
	@Test(expected=DataFlowException.class)
	public void testResetPredicate() throws ParseException, DataFlowException{
	    constructIndexBasedSourceOperator(TestConstants.DESCRIPTION + ":angry");
	    indexBasedSourceOperator.open();
	    indexBasedSourceOperator.getNextTuple();
	    indexBasedSourceOperator.resetPredicate(dataReaderPredicate);
	    indexBasedSourceOperator.getNextTuple();
	}
}
