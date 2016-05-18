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

    private FuzzyTokenMatcher fuzzyMatcher;
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
    
    public List<ITuple> getResults(String query,double threshold, ArrayList<Attribute> attributeList, boolean getSpan) throws DataFlowException, ParseException {

        IPredicate predicate = new FuzzyTokenPredicate(query, attributeList, analyzer, dataStore, threshold, getSpan);
        fuzzyMatcher = new FuzzyTokenMatcher(predicate);
        fuzzyMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;
        while ((nextTuple = fuzzyMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }
    
    @Test
    public void FuzzyTokenMatcherWithNoResults() throws Exception {
        String query = "Twelve Angry Men Cafe";
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean getSpan = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        List<ITuple> results = getResults(query, threshold, attributeList, getSpan);
        //Assert.assertEquals(0,results.size());
    }
    
    @Test
    public void FuzzyTokenMatcherForStringField() throws Exception {
        String query = "tom hanks";
        double threshold = 1; 	//The ratio of tokens that need to be matched
        boolean getSpan = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        List<ITuple> results = getResults(query, threshold, attributeList, getSpan);
        //Assert.assertEquals(0,results.size());
    }
    
    @Test
    public void FuzzyTokenMatcherWithoutSpan() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean getSpan = false;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length];
        for(int count = 0; count < schemaAttributes.length; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields4 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);
        
        List<ITuple> results = getResults(query, threshold, attributeList, getSpan);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        //Assert.assertTrue(contains);
        //Assert.assertEquals(3,results.size());
    }
    
    @Test
    public void FuzzyTokenMatcherWithSpan() throws Exception {
        String query = "Twelve Angry Men";
        double threshold = 0.5; 	//The ratio of tokens that need to be matched
        boolean getSpan = true;
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.DESCRIPTION_ATTR);
        
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        
        List<Span> list = new ArrayList<>();
        Span span = new Span(TestConstants.FIRST_NAME, 5, 10, "Angry", "Angry");
        list.add(span);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        
        list.clear();
        span = new Span(TestConstants.FIRST_NAME, 6, 11, "Angry", "Angry");
        list.add(span);
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list) };
        
        list.clear();
        span = new Span(TestConstants.FIRST_NAME, 39, 44, "Angry", "Angry");
        list.add(span);
        IField[] fields4 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list)};
        
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple3);
        expectedResultList.add(tuple4);
        
        List<ITuple> results = getResults(query, threshold, attributeList, getSpan);
        boolean contains = TestUtils.containsAllResults(expectedResultList, results);
        //Assert.assertTrue(contains);
        //Assert.assertEquals(3,results.size());
    }
}
