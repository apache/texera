package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;

/**
 * Unit tests for RegexMatcher. Integration tests with RegexToGramTranslator.
 * 
 * @author Shuying Lai
 * @author Zuozhi Wang
 * @author Qing Tang
 */
public class RegexMatcherTest {
    
    public static final String PEOPLE_TABLE = RegexMatcherTestHelper.PEOPLE_TABLE;
    public static final String CORP_TABLE = RegexMatcherTestHelper.CORP_TABLE;
    public static final String STAFF_TABLE = RegexMatcherTestHelper.STAFF_TABLE;
    public static final String TEXT_TABLE = RegexMatcherTestHelper.TEXT_TABLE;
    
    public static final String RESULTS = RegexMatcherTestHelper.RESULTS;
    
    @BeforeClass
    public static void setUp() throws TexeraException {
        RegexMatcherTestHelper.writeTestTables();
    }
    
    @AfterClass
    public static void cleanUp() throws TexeraException {
        RegexMatcherTestHelper.deleteTestTables();
    }

    @Test
    public void testGetNextTuplePeopleFirstName() throws Exception {
        String query = "g[^\\s]*";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, Arrays.asList(TestConstants.FIRST_NAME));

        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "brad lie angelina"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();

        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(TestConstants.FIRST_NAME, 11, 17, query, "gelina"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(2).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "george lin lin"
        spans.clear();
        spans.add(new Span(TestConstants.FIRST_NAME, 0, 6, query, "george"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testGetNextTupleCorpURL() throws Exception {
        String query = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                CORP_TABLE, query, Arrays.asList(RegexTestConstantsCorp.URL));

        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "http://weibo.com"
        List<Tuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsCorp.SCHEMA_CORP).add(RESULTS, AttributeType.LIST).build();

        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsCorp.URL, 0, 16, query, "http://weibo.com"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(1).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "https://www.microsoft.com/en-us/"
        spans.clear();
        spans.add(new Span(RegexTestConstantsCorp.URL, 0, 32, query, "https://www.microsoft.com/en-us/"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(2).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testGetNextTupleCorpIP() throws Exception {
        String query = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                CORP_TABLE, query, Arrays.asList(RegexTestConstantsCorp.IP_ADDRESS));

        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "66.220.144.0"
        List<Tuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsCorp.SCHEMA_CORP).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "66.220.144.0"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "180.149.134.141"
        spans.clear();
        spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 15, query, "180.149.134.141"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(1).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "131.107.0.89"
        spans.clear();
        spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "131.107.0.89"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(2).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));

    }

    @Test
    public void testGetNextTupleStaffEmail() throws Exception {
        String query = "^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                STAFF_TABLE, query, Arrays.asList(RegexTestConstantStaff.EMAIL));

        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "k.bocanegra@uci.edu"
        List<Tuple> data = RegexTestConstantStaff.getSampleStaffTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantStaff.SCHEMA_STAFF).add(RESULTS, AttributeType.LIST).build();

        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 19, query, "m.bocanegra@164.com"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "hwangl@ics.uci.edu"
        spans.clear();
        spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 18, query, "hwangk@ske.akb.edu"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(1).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testRegexText1() throws Exception {
        String query = "test(er|ing|ed|s)?";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT));
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "test" & testing"
        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();

        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 5, 9, query, "test"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 21, 28, query, "testing"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "tests"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 87, 92, query, "tests"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(2).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "tested"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 43, 49, query, "tested"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testRegexText2() throws Exception {
        String query = "follow(-| )?up";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT));
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "followup"
        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 28, 36, query, "followup"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 54, 62, query, "followup"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(4).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "follow up"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 18, 27, query, "follow up"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 51, 60, query, "follow up"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "follow-up" & "followup"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 24, 33, query, "follow-up"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 38, 46, query, "followup"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(6).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testRegexText3() throws Exception {
        String query = "([a-zA-Z])+o[a-z]a[a-z]o";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT));
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match "Tomato" & "tomato"
        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 6, query, "Tomato"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 94, 100, query, "tomato"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(7).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "Potato"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 6, query, "Potato"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(8).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        // expected to match "avocado"
        spans.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 53, 60, query, "avocado"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(9).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testRegexText4() throws Exception {
        String query = "\\[(.)?\\]";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT));
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        // expected to match [a] & [!]
        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 110, 113, query, "[a]"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 120, 123, query, "[!]"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(10).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }

    @Test
    public void testRegexWithLimit() throws Exception {
        String query = "patient";
        List<Tuple> exactResultsWithLimit = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT), true, 2, 0);
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(4).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        spans.clear();
        fields.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 65, 72, query, "patient"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        spans.clear();
        fields.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(6).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.containsAll(expectedResults, exactResultsWithLimit));
        Assert.assertEquals(expectedResults.size(), 3);
        Assert.assertEquals(exactResultsWithLimit.size(), 2);
    }

    @Test
    public void testRegexWithLimitOffset() throws Exception {
        String query = "patient";
        List<Tuple> exactResultsWithLimitOffset = RegexMatcherTestHelper.getQueryResults(
                TEXT_TABLE, query, Arrays.asList(RegexTestConstantsText.CONTENT), true, 2, 1);
        
        List<Tuple> expectedResults = new ArrayList<Tuple>();

        List<Tuple> data = RegexTestConstantsText.getSampleTextTuples();
        Schema spanSchema = new Schema.Builder().add(RegexTestConstantsText.SCHEMA_TEXT).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<Span>();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
        List<IField> fields = new ArrayList<IField>(data.get(4).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        spans.clear();
        fields.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        spans.add(new Span(RegexTestConstantsText.CONTENT, 65, 72, query, "patient"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        spans.clear();
        fields.clear();
        spans.add(new Span(RegexTestConstantsText.CONTENT, 4, 11, query, "patient"));
        spanField = new ListField<Span>(new ArrayList<Span>(spans));
        fields = new ArrayList<IField>(data.get(6).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        Assert.assertTrue(TestUtils.containsAll(expectedResults, exactResultsWithLimitOffset));
        Assert.assertEquals(expectedResults.size(), 3);
        Assert.assertEquals(exactResultsWithLimitOffset.size(), 2);
    }

    // @Test
    // public void testRegexWithLimitProblem() throws Exception {
    // List<ITuple> data = RegexTestConstantsText.getSampleTextTuples();
    // RegexMatcherTestHelper testHelper = new
    // RegexMatcherTestHelper(RegexTestConstantsText.SCHEMA_TEXT, data);
    //
    // String regex = "(T|t)he";
    // testHelper.runTest(regex, RegexTestConstantsText.CONTENT, true);
    //
    // List<ITuple> exactResults = testHelper.getResults();
    // List<ITuple> expectedResults = new ArrayList<ITuple>();
    //
    // Schema spanSchema = testHelper.getSpanSchema();
    // List<Span> spans = new ArrayList<Span>();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 61, 64, regex,
    // "the"));
    // IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 3, regex, "The"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(4).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 3, regex, "The"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(5).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 0, 3, regex, "The"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(6).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 10, 13, regex,
    // "the"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(7).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 75, 78, regex,
    // "the"));
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 110, 113, regex,
    // "the"));
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 132, 135, regex,
    // "the"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(8).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    //
    // spans.clear();
    // fields.clear();
    // spans.add(new Span(RegexTestConstantsText.CONTENT, 70, 73, regex,
    // "the"));
    // spanField = new ListField<Span>(new ArrayList<Span>(spans));
    // fields = new ArrayList<IField>(data.get(9).getFields());
    // fields.add(spanField);
    // expectedResults.add(new DataTuple(spanSchema, fields.toArray(new
    // IField[fields.size()])));
    // System.out.println(expectedResults);
    // System.out.println(Utils.getTupleListString(exactResults));
    //
    // Assert.assertTrue(expectedResults.containsAll(exactResults));
    // Assert.assertEquals(exactResults.size(), 3);
    // }

}
