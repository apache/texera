package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author Parag Saraogi
 *
 */

public class FuzzyTokenMatcherTest {

    private FuzzyTokenMatcher fuzzyTokenMatcher;
    private IDataWriter dataWriter;
    private DataStore dataStore;
    private Analyzer analyzer;

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, analyzer);
        dataWriter.clearData();
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
        }
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    public List<ITuple> getQueryResults(String query, double threshold, ArrayList<Attribute> attributeList)
            throws DataFlowException, ParseException {
        return getQueryResults(query, threshold, attributeList, Integer.MAX_VALUE, 0);
    }

    public List<ITuple> getQueryResults(String query, double threshold, ArrayList<Attribute> attributeList,
            int limit) throws DataFlowException, ParseException {
        return getQueryResults(query, threshold, attributeList, limit, 0);
    }

    public List<ITuple> getQueryResults(String query, double threshold, ArrayList<Attribute> attributeList,
            int limit, int offset) throws DataFlowException, ParseException {

        FuzzyTokenPredicate predicate = new FuzzyTokenPredicate(query, attributeList, analyzer, threshold);
        fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);
        fuzzyTokenMatcher.setInputOperator(new IndexBasedSourceOperator(predicate.getDataReaderPredicate(dataStore)));
        fuzzyTokenMatcher.open();
        fuzzyTokenMatcher.setLimit(limit);
        fuzzyTokenMatcher.setOffset(offset);

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;
        while ((nextTuple = fuzzyTokenMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }

    @Test
    public void TestFuzzyTokenMatcherWithNoResults() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        List<ITuple> results = getQueryResults(query, threshold, attributeList);
        
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void TestFuzzyTokenMatcherWithThresholdVariation() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.25;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Schema spanSchema = Utils.createSpanSchema(TestConstants.SCHEMA_PEOPLE);

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

        ITuple tuple1 = new DataTuple(spanSchema, fields1);
        ITuple tuple2 = new DataTuple(spanSchema, fields2);
        ITuple tuple3 = new DataTuple(spanSchema, fields3);
        ITuple tuple4 = new DataTuple(spanSchema, fields4);
        
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = getQueryResults(query, threshold, attributeList);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcherWithLargeTokens() throws Exception {
        String query = "Twelve Angry Men Came Cafe Have Coffee Eat Chocolate Burger Fries SandWidch Cool Food Drinks American drama film elements film noir adapted teleplay same name Reginald Rose Written Rose directed  Sidney Lumet trial film tells story jury made deliberate guilt acquittal defendant basis reasonable doubt United States verdict most criminal ";
        double threshold = 0.02;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Schema spanSchema = Utils.createSpanSchema(TestConstants.SCHEMA_PEOPLE);

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

        ITuple tuple1 = new DataTuple(spanSchema, fields1);
        ITuple tuple2 = new DataTuple(spanSchema, fields2);
        ITuple tuple3 = new DataTuple(spanSchema, fields3);
        ITuple tuple4 = new DataTuple(spanSchema, fields4);

        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = getQueryResults(query, threshold, attributeList);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcherForStringField() throws Exception {
        String query = "tom hanks";
        double threshold = 1; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        List<ITuple> results = getQueryResults(query, threshold, attributeList);

        Assert.assertEquals(0, results.size());
    }

    @Test
    public void TestFuzzyTokenMatcher1() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Schema spanSchema = Utils.createSpanSchema(TestConstants.SCHEMA_PEOPLE);

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

        ITuple tuple1 = new DataTuple(spanSchema, fields1);
        ITuple tuple2 = new DataTuple(spanSchema, fields2);
        ITuple tuple3 = new DataTuple(spanSchema, fields3);
        ITuple tuple4 = new DataTuple(spanSchema, fields4);

        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = getQueryResults(query, threshold, attributeList);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }

    @Test
    public void TestFuzzyTokenMatcher2() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Schema spanSchema = Utils.createSpanSchema(TestConstants.SCHEMA_PEOPLE);

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

        ITuple tuple1 = new DataTuple(spanSchema, fields1);
        ITuple tuple2 = new DataTuple(spanSchema, fields2);
        ITuple tuple3 = new DataTuple(spanSchema, fields3);
        ITuple tuple4 = new DataTuple(spanSchema, fields4);

        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = getQueryResults(query, threshold, attributeList);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithLimit() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

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

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = Utils
                .removePayload(getQueryResults(query, threshold, attributeList, 2));
        Assert.assertEquals(expectedResultList.size(), 4);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(expectedResultList.containsAll(results));
    }

    @Test
    public void TestFuzzyTokenMatcherWithLimitOffset() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; // The ratio of tokens that need to be matched
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

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

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);

        List<ITuple> results = Utils
                .removePayload(getQueryResults(query, threshold, attributeList, 2, 1));
        Assert.assertEquals(expectedResultList.size(), 4);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(expectedResultList.containsAll(results));
    }
}
