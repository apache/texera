package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;

/**
 * Unit tests for RegexMatcher.
 * Integration tests with RegexToGramTranslator.
 * 
 * @author Shuying Lai
 * @author Zuozhi Wang
 * @author Qing Tang
 */
public class RegexMatcherTest {
	
	// Helper function to print results for debugging purposes
	private void printResults(List<ITuple> results) {
		for (ITuple result : results) {
			List<Span> a = ((ListField<Span>) result.getField("spanList")).getValue();
			for (Span i : a) {
				System.out.printf("start: %d, end: %d, fieldName: %s, key: %s, value: %s\n", 
						i.getStart(), i.getEnd(), i.getFieldName(), i.getKey(), i.getValue());
			}
		}
		System.out.println();
	}
	
	
	@Test
	public void testGetNextTuplePeopleFirstName() throws Exception {
		List<ITuple> data = TestConstants.getSamplePeopleTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(TestConstants.SCHEMA_PEOPLE, data);
		
		testHelper.runTest("g[^\\s]*", TestConstants.FIRST_NAME_ATTR);
		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match "brad lie angelina"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(TestConstants.FIRST_NAME, 11, 17, "g[^\\s]*", "gelina"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "george lin lin"
		spans.clear();
		spans.add(new Span(TestConstants.FIRST_NAME, 0, 6, "g[^\\s]*", "george"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(3).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
				
		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpURL() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);
		
		String query = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
		testHelper.runTest(query, RegexTestConstantsCorp.URL_ATTR);
		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();

		//expected to match "http://weibo.com"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsCorp.URL, 0, 16, query, "http://weibo.com"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "https://www.microsoft.com/en-us/"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.URL, 0, 32, query, "https://www.microsoft.com/en-us/"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));

		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpIP() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);

		String query = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
		testHelper.runTest(query, RegexTestConstantsCorp.IP_ADDRESS_ATTR);
		List<ITuple> exactResults = testHelper.getResults();

		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match "66.220.144.0"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "66.220.144.0"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "180.149.134.141"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 15, query, "180.149.134.141"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "131.107.0.89"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "131.107.0.89"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));

		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleStaffEmail() throws Exception {
		List<ITuple> data = RegexTestConstantStaff.getSampleStaffTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantStaff.SCHEMA_STAFF, data);
		
		String query = "^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$";
		testHelper.runTest(query, RegexTestConstantStaff.EMAIL_ATTR);
		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match "k.bocanegra@uci.edu"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 19, query, "m.bocanegra@164.com"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "hwangl@ics.uci.edu"
		spans.clear();
		spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 18, query, "hwangk@ske.akb.edu"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
				
		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testRegexText1() throws Exception {
		List<ITuple> data = RegexTestConstantsText.getSampleTextTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsText.SCHEMA_TEXT, data);
		
		String regex = "test(er|ing|ed|s)?";

		testHelper.runTest(regex, RegexTestConstantsText.CONTENT_ATTR, true);

		List<ITuple> exactResults = testHelper.getResults();	
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		
		//expected to match "test" & testing"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 5, 9, regex, "test"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 21, 28, regex, "testing"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "tests"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 87, 92, regex, "tests"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "tested"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 43, 49, regex, "tested"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(3).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));

		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testRegexText2() throws Exception {
		List<ITuple> data = RegexTestConstantsText.getSampleTextTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsText.SCHEMA_TEXT, data);
		
		String regex = "follow(-| )?up";
		testHelper.runTest(regex, RegexTestConstantsText.CONTENT_ATTR, true);

		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match "followup"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 28, 36, regex, "followup"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 54, 62, regex, "followup"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(4).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "follow up"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 18, 27, regex, "follow up"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 51, 60, regex, "follow up"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(5).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "follow-up" & "followup"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 24, 33, regex, "follow-up"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 38, 46, regex, "followup"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(6).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}

	
	@Test
	public void testRegexText3() throws Exception {
		List<ITuple> data = RegexTestConstantsText.getSampleTextTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsText.SCHEMA_TEXT, data);
		
		String regex = "([a-zA-Z])+o[a-z]a[a-z]o";
		testHelper.runTest(regex, RegexTestConstantsText.CONTENT_ATTR, true);

		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match "Tomato" & "tomato"
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 6, regex, "Tomato"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 94, 100, regex, "tomato"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(7).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "Potato"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 6, regex, "Potato"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(8).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "avocado"
		spans.clear();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 53, 60, regex, "avocado"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(9).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testRegexText4() throws Exception {
		List<ITuple> data = RegexTestConstantsText.getSampleTextTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsText.SCHEMA_TEXT, data);
		
		String regex = "\\[(.)?\\]";
		testHelper.runTest(regex, RegexTestConstantsText.CONTENT_ATTR, true);

		List<ITuple> exactResults = testHelper.getResults();
		
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		
		//expected to match [a] & [!]
		Schema spanSchema = testHelper.getSpanSchema();
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsText.CONTENT, 110, 113, regex, "[a]"));
		spans.add(new Span(RegexTestConstantsText.CONTENT, 120, 123, regex, "[!]"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(10).getFields());
		fields.add(spanField);
		expectedResults.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(TestUtils.containsAllResults(expectedResults, exactResults));
		
		testHelper.cleanUp();
	}
	
}



