package edu.uci.ics.texera.dataflow.regexmatcher.label;

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
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcherTestHelper;

/**
 * Unit tests for LabeledRegexMatcher.
 *
 * @author Bhushan Pagariya (bhushanpagariya)
 * @author Harshini Shah
 * @author Yashaswini Amaresh
 */

public class LabeledRegexMatcherTest {
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
    public void testGetNextTupleLabeledRegex() throws Exception {
        String query = "<name>";
        String keywordQuery = "george lin lin";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, keywordQuery, Arrays.asList(TestConstants.FIRST_NAME), "name", false, Integer.MAX_VALUE, 0);

        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "george lin lin"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();

        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.FIRST_NAME, 0, 14, query, "george lin lin"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }

    @Test
    public void testIgnoreCaseLabeledRegex() throws Exception {
        String query = "<NAME>";
        String keywordQuery = "george lin lin";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, keywordQuery, Arrays.asList(TestConstants.FIRST_NAME), "name", false, Integer.MAX_VALUE, 0);

        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "george lin lin"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.FIRST_NAME, 0, 14, query, "george lin lin"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }

    @Test
    public void testEscapeCharacterLabeledRegex() throws Exception {
        String query = "\\<name\\>";
        String keywordQuery = "george lin lin";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, keywordQuery, Arrays.asList(TestConstants.FIRST_NAME), "name", false, Integer.MAX_VALUE, 0);
        // exactResult should not contain any tuple
        Assert.assertTrue(exactResults.size()==0);
    }

    @Test
    public void testMultipleLabeledRegex() throws Exception {
        String query = "<lab1>.*<lab2>";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, "short", Arrays.asList(TestConstants.DESCRIPTION), "lab1", false, Integer.MAX_VALUE, 0, "angry", "lab2");

        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "Short angry" and "Short and lin clooney is Angry"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.DESCRIPTION, 0, 11, query, "Short angry"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        spans.clear();
        spans.add(new Span(TestConstants.DESCRIPTION, 15, 45, query, "Short and lin clooney is Angry"));
        spanField = new ListField<>(new ArrayList<>(spans));
        fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }

    @Test
    public void testDisjunctionLabeledRegex() throws Exception {
        String query = "<lab1>|<lab2>";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, "short", Arrays.asList(TestConstants.DESCRIPTION), "lab1", false, Integer.MAX_VALUE, 0, "angry", "lab2");

        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "Short" and "angry"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.DESCRIPTION, 0, 5, query, "Short"));
        spans.add(new Span(TestConstants.DESCRIPTION, 6, 11, query, "angry"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));
        // expected to match "Short" and "Angry"
        spans.clear();
        spans.add(new Span(TestConstants.DESCRIPTION, 15, 20, query, "Short"));
        spans.add(new Span(TestConstants.DESCRIPTION, 40, 45, query, "Angry"));
        spanField = new ListField<>(new ArrayList<>(spans));
        fields = new ArrayList<>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }

    @Test
    public void testQueryWithoutQualifiersLabeledRegex1() throws Exception{
        String query = "<lab1> <lab2>";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, "short", Arrays.asList(TestConstants.DESCRIPTION), "lab1", false, Integer.MAX_VALUE, 0, "angry", "lab2");
        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "Short angry"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.DESCRIPTION, 0, 11, query, "Short angry"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(5).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }
    @Test
    public void testQueryWithoutQualifiersLabeledRegex2() throws Exception{
        String query = "<lab2> is <lab1>";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, "short", Arrays.asList(TestConstants.DESCRIPTION), "lab1", false, Integer.MAX_VALUE, 0, "Clooney", "lab2");
        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "Short angry"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.DESCRIPTION, 4, 20, query, "Clooney is Short"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }
    @Test
    public void testQueryWithoutQualifiersLabeledRegex3() throws Exception{
        String query = "Lin <lab2> is <lab1> and lin <lab2> is Angry";
        List<Tuple> exactResults = RegexMatcherTestHelper.getQueryResults(
                PEOPLE_TABLE, query, "short", Arrays.asList(TestConstants.DESCRIPTION), "lab1", false, Integer.MAX_VALUE, 0, "Clooney", "lab2");
        List<Tuple> expectedResults = new ArrayList<>();

        // expected to match "Short angry"
        List<Tuple> data = TestConstants.getSamplePeopleTuples();
        Schema spanSchema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS, AttributeType.LIST).build();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(TestConstants.DESCRIPTION, 0, 45, query, "Lin Clooney is Short and lin clooney is Angry"));
        IField spanField = new ListField<>(new ArrayList<>(spans));
        List<IField> fields = new ArrayList<>(data.get(3).getFields());
        fields.add(spanField);
        expectedResults.add(new Tuple(spanSchema, fields.toArray(new IField[fields.size()])));

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(RESULTS);
        Assert.assertTrue(TestUtils.attributeEquals(expectedResults, exactResults, attributeNames));
    }

}
