package edu.uci.ics.textdb.dataflow.keywordmatch;

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
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author Prakul
 *
 */

public class KeywordMatcherTest {

    private KeywordMatcher keywordMatcher;
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

    /**
     * For a given string query & list of attributes it gets a list of results
     * buildMultiQueryOnAttributeList flag decides if the query is formed as a boolean Query on all attribute
     * or all records are scanned
     * @param query
     * @param attributeList
     * @return List<ITuple>
     * @throws DataFlowException
     * @throws ParseException
     */

    public List<ITuple> getPeopleQueryResults(String query, ArrayList<Attribute> attributeList) throws DataFlowException, ParseException {

        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, analyzer, dataStore);
        keywordMatcher = new KeywordMatcher(predicate);
        keywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }


    /**
     * Verifies Keyword Matcher on multiword string. Since both tokens in Query "short tall" don't exist in
     * any single document, it should not return any tuple.
     * @throws Exception
     */
    @Test
    public void testKeywordMatcher() throws Exception {
        //Prepare Query
        String query = "short TAll";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Perform Query
        List<ITuple> results = getPeopleQueryResults(query, attributeList);

        //Perform Check
        Assert.assertEquals(0,results.size());
    }

    /**
     * Verifies GetNextTuple of Keyword Matcher and single
     * word queries in String Field
     * @throws Exception
     */
    @Test
    public void testSingleWordQueryInStringField() throws Exception {
        //Prepare Query
        String query = "bruce";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }


    /**
     * Verifies GetNextTuple of Keyword Matcher and single
     * word queries in Text Field
     * @throws Exception
     */

    @Test
    public void testSingleWordQueryInTextField() throws Exception {
        //Prepare Query
        String query = "TaLL";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 0, 4, "TaLL", "Tall",0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];

        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }

        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };

        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);

        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }


    /**
     * Verifies List<ITuple> returned by Keyword Matcher on multiple
     * word queries
     * @throws Exception
     */
    @Test
    public void testMultipleWordsQuery() throws Exception {
        //Prepare Query
        String query = "george lin lin";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }


    /**
     * Verifies: data source has multiple attributes, and an entity
     * can appear in all the fields and multiple times.
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQuery() throws Exception {
        //Prepare Query
        String query = "lin clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 3, "lin", "Lin", 0);
        Span span3 = new Span("description", 25, 28, "lin", "lin", 5);
        Span span4 = new Span("description", 4, 11, "clooney", "Clooney", 1);
        Span span5 = new Span("description", 29, 36, "clooney", "clooney", 6);
        list.add(span1);
        list.add(span2);
        list.add(span3);
        list.add(span4);
        list.add(span5);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for(int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: All tokens of Query should appear in a Single Field of each document in Data source
     * otherwise it doesnt return anything
     *
     * Ex: For Document:
     *  new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
     new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
     new TextField("Lin Clooney is Short and lin clooney is Angry")

     For Query : george clooney

     Result: Nothing should be returned as george and clooney exist in different fields of same document
     * @throws Exception
     */
    @Test
    public void testQueryWordsFoundInMultipleFields() throws Exception {
        //Prepare Query
        String query = "george clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        //Perform Check
        Assert.assertEquals(0,resultList.size());

    }

}
