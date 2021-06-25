package edu.uci.ics.texera.dataflow.fuzzytokenmatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;

/**
 * @author Parag Saraogi
 * @author Zuozhi Wang
 */
public class FuzzyTokenMatcherTest {
    
    public static final String PEOPLE_TABLE = FuzzyTokenMatcherTestHelper.PEOPLE_TABLE;
    public static final String RESULTS = FuzzyTokenMatcherTestHelper.RESULTS;
    public static final Attribute RESULTS_ATTR = new Attribute(RESULTS, AttributeType.LIST);

    @BeforeClass
    public static void setUp() throws Exception {
        FuzzyTokenMatcherTestHelper.writeTestTables();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        FuzzyTokenMatcherTestHelper.deleteTestTables();
    }

    @Test
    public void TestFuzzyTokenMatcherWithNoResults() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);
        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);
        
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void TestFuzzyTokenMatcherWithThresholdVariation() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.25;
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();

        List<Span> spanList1 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1));
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(spanList1) };
        
        List<Span> spanList2 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1));
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<Span>(spanList2) };
        
        List<Span> spanList3 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8));
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(spanList3) };
        
        List<Span> spanList4 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1));
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<Span>(spanList4) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);
        
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);
        boolean contains = TestUtils.equals(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcherWithLargeTokens() throws Exception {
        String query = "Twelve Angry Men Came Cafe Have Coffee Eat Chocolate Burger Fries SandWidch Cool Food Drinks American drama film elements film noir adapted teleplay same name Reginald Rose Written Rose directed  Sidney Lumet trial film tells story jury made deliberate guilt acquittal defendant basis reasonable doubt United States verdict most criminal ";
        double threshold = 0.02;
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();


        List<Span> spanList1 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1));
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(spanList1) };
        
        List<Span> spanList2 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1));
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<Span>(spanList2) };
        
        List<Span> spanList3 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8));
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(spanList3) };
        
        List<Span> spanList4 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1));
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<Span>(spanList4) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);

        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);
        boolean contains = TestUtils.equals(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcherForStringField() throws Exception {
        String query = "tom hanks";
        double threshold = 1; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);

        Assert.assertEquals(0, results.size());
    }

    @Test
    public void TestFuzzyTokenMatcher1() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();

        List<Span> spanList1 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1));
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(spanList1) };
        
        List<Span> spanList2 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1));
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<Span>(spanList2) };
        
        List<Span> spanList3 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8));
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(spanList3) };
        
        List<Span> spanList4 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1));
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<Span>(spanList4) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);

        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);
        boolean contains = TestUtils.equals(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcher2() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();

        List<Span> spanList1 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1));
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(spanList1) };
        
        List<Span> spanList2 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1));
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<Span>(spanList2) };
        
        List<Span> spanList3 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8));
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(spanList3) };
        
        List<Span> spanList4 = Arrays.asList(
                new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1));
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<Span>(spanList4) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);

        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames);
        boolean contains = TestUtils.equals(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithLimit() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();

        List<Span> list = new ArrayList<>();
        Span span = new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1);
        list.add(span);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1);
        list.add(span);
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8);
        list.add(span);
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1);
        list.add(span);
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<>(list) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);
        
        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames, 2, 0);

        Assert.assertEquals(expectedResultList.size(), 4);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(TestUtils.containsAll(expectedResultList, results));
    }

    @Test
    public void TestFuzzyTokenMatcherWithLimitOffset() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.DESCRIPTION);

        Schema schema = new Schema.Builder().add(TestConstants.SCHEMA_PEOPLE).add(RESULTS_ATTR).build();

        List<Span> list = new ArrayList<>();
        Span span = new Span(TestConstants.DESCRIPTION, 5, 10, "angry", "Angry", 1);
        list.add(span);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "Angry", 1);
        list.add(span);
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 40, 45, "angry", "Angry", 8);
        list.add(span);
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 6, 11, "angry", "angry", 1);
        list.add(span);
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<>(list) };

        Tuple tuple1 = new Tuple(schema, fields1);
        Tuple tuple2 = new Tuple(schema, fields2);
        Tuple tuple3 = new Tuple(schema, fields3);
        Tuple tuple4 = new Tuple(schema, fields4);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<Tuple> results = FuzzyTokenMatcherTestHelper.getQueryResults(PEOPLE_TABLE, query, threshold, attributeNames, 2, 1);

        Assert.assertEquals(expectedResultList.size(), 4);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(TestUtils.containsAll(expectedResultList, results));
    }
}
