package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import edu.uci.ics.textdb.api.common.IPredicate;
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
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
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
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
    }
    
    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }
    
    public List<ITuple> getQueryResults(String query,double threshold, ArrayList<Attribute> attributeList, boolean isSpanInformationAdded) throws DataFlowException, ParseException {

        IPredicate predicate = new FuzzyTokenPredicate(query, attributeList, analyzer, dataStore, threshold, isSpanInformationAdded);
        fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);
        fuzzyTokenMatcher.open();

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
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean isSpanInformationAdded = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        Assert.assertEquals(0,results.size());
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithThresholdVariation() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.25;
        boolean isSpanInformationAdded = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
 
        Attribute[] schemaAttributes = TestConstants.ATTRIBUTES_PEOPLE;
        
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithLargeTokens() throws Exception {
        String query = "Twelve Angry Men Came Cafe Have Coffee Eat Chocolate Burger Fries SandWidch Cool Food Drinks American drama film elements film noir adapted teleplay same name Reginald Rose Written Rose directed  Sidney Lumet trial film tells story jury made deliberate guilt acquittal defendant basis reasonable doubt United States verdict most criminal ";
        double threshold = 0.02;
        boolean isSpanInformationAdded = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
 
        Attribute[] schemaAttributes = TestConstants.ATTRIBUTES_PEOPLE;
        
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void TestFuzzyTokenMatcherForStringField() throws Exception {
        String query = "tom hanks";
        double threshold = 1; 	//The ratio of tokens that need to be matched
        boolean isSpanInformationAdded = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        List<ITuple> expectedResultList = new ArrayList<>();
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
        Assert.assertEquals(0,results.size());
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithoutSpan() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean isSpanInformationAdded = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        
        Attribute[] schemaAttributes = TestConstants.ATTRIBUTES_PEOPLE;
        
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void TestFuzzyTokenMatcherWithSpan() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean isSpanInformationAdded = true;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        
        List<Span> list = new ArrayList<>();
        Span span = new Span(TestConstants.DESCRIPTION, 5, 10, "Angry", "Angry", 1);
        list.add(span);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        
        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 6, 11, "Angry", "Angry", 1);
        list.add(span);
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list) };
        
        list = new ArrayList<>();
        span = new Span(TestConstants.DESCRIPTION, 40, 45, "Angry", "Angry", 8);
        list.add(span);
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list)};
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);
        expectedResultList.add(tuple3);
        
        List<ITuple> results = getQueryResults(query, threshold, attributeList, isSpanInformationAdded);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        Assert.assertTrue(contains);
    }
}
