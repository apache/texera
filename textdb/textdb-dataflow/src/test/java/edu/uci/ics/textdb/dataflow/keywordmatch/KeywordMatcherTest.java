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
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author Prakul
 *
 */

public class KeywordMatcherTest {

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

    /**
     * For a given string query & list of attributes it gets a list of results
     * buildMultiQueryOnAttributeList flag decides if the query is formed as a
     * boolean Query on all attribute or all records are scanned
     * 
     * @param query
     * @param attributeList
     * @return List<ITuple>
     * @throws DataFlowException
     * @throws ParseException
     */

    public List<ITuple> getPeopleQueryResults(String query, ArrayList<Attribute> attributeList)
            throws DataFlowException, ParseException {

        return getPeopleQueryResults(query, attributeList, Integer.MAX_VALUE, 0);
    }

    public List<ITuple> getPeopleQueryResults(String query, ArrayList<Attribute> attributeList, int limit)
            throws DataFlowException, ParseException {

        return getPeopleQueryResults(query, attributeList, limit, 0);
    }

    public List<ITuple> getPeopleQueryResults(String query, ArrayList<Attribute> attributeList, int limit, int offset)
            throws DataFlowException, ParseException {

        KeywordPredicate keywordPredicate = new KeywordPredicate(query, attributeList, analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        keywordSource.open();
        keywordSource.setLimit(limit);
        keywordSource.setOffset(offset);

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = keywordSource.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }


    /**
     * Verifies Keyword Matcher on a multi-word string. Since both tokens in Query
     * "short tall" don't exist in any single document, it should not return any
     * tuple.
     * 
     * @throws Exception
     */
    @Test
    public void testKeywordMatcher() throws Exception {
        // Prepare the query
        String query = "short TAll";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Perform the query
        List<ITuple> results = getPeopleQueryResults(query, attributeList);

        // check the results
        Assert.assertEquals(0, results.size());
    }

    /**
     * Verifies GetNextTuple of Keyword Matcher and single word queries in
     * String Field
     * 
     * @throws Exception
     */
    @Test
    public void testSingleWordQueryInStringField() throws Exception {
        // Prepare the query
        String query = "bruce";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // check the results
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies GetNextTuple of Keyword Matcher and single word queries in Text
     * Field
     * 
     * @throws Exception
     */
    @Test
    public void testSingleWordQueryInTextField() throws Exception {
        // Prepare the query
        String query = "TaLL";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 0, 4, "tall", "Tall", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];

        for (int count = 0; count < schemaAttributes.length - 1; count++) {
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

        // Perform the query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // check the results
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies the List<ITuple> returned by Keyword Matcher on multiple-word
     * queries
     * 
     * @throws Exception
     */
    @Test
    public void testMultipleWordsQuery() throws Exception {
        // Prepare the query
        String query = "george lin lin";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare the expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // check the results
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: data source has multiple attributes, and an entity can appear
     * in all the fields and multiple times.
     * 
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQuery() throws Exception {
        // Prepare the query
        String query = "lin clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare the expected result list
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
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // check the results
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: All tokens of Query should appear in a Single Field of each
     * document in Data source otherwise it doesnt return anything
     *
     * Ex: For Document: new StringField("george lin lin"), new StringField("lin
     * clooney"), new IntegerField(43), new DoubleField(6.06), new DateField(new
     * SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")), new TextField("Lin
     * Clooney is Short and lin clooney is Angry")
     * 
     * For Query : george clooney
     * 
     * Result: Nothing should be returned as george and clooney exist in
     * different fields of same document
     * 
     * @throws Exception
     */
    @Test
    public void testQueryWordsFoundInMultipleFields() throws Exception {
        // Prepare the query
        String query = "george clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Perform the query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // Check the results
        Assert.assertEquals(0, resultList.size());

    }

    @Test
    public void testMatchingWithLimit() throws DataFlowException, ParseException, java.text.ParseException {
        String query = "angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<ITuple> resultList = getPeopleQueryResults(query, attributeList, 3);
        List<ITuple> expectedList = new ArrayList<>();

        Span span1 = new Span("description", 5, 10, "angry", "Angry", 1);
        Span span2 = new Span("description", 6, 11, "angry", "Angry", 1);
        Span span3 = new Span("description", 40, 45, "angry", "Angry", 8);
        Span span4 = new Span("description", 6, 11, "angry", "angry", 1);

        List<Span> list1 = new ArrayList<>();
        list1.add(span1);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list1) };
        List<Span> list2 = new ArrayList<>();
        list2.add(span2);
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list2) };

        List<Span> list3 = new ArrayList<>();
        list3.add(span3);
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list3) };

        List<Span> list4 = new ArrayList<>();
        list4.add(span4);
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<>(list4) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        resultList = Utils.removePayload(resultList);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 3);
        Assert.assertTrue(expectedList.containsAll(resultList));
    }

    @Test
    public void testMatchingWithLimitOffset() throws DataFlowException, ParseException, java.text.ParseException {
        String query = "angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<ITuple> resultList = getPeopleQueryResults(query, attributeList, 3, 2);
        List<ITuple> expectedList = new ArrayList<>();

        Span span1 = new Span("description", 5, 10, "angry", "Angry", 1);
        Span span2 = new Span("description", 6, 11, "angry", "Angry", 1);
        Span span3 = new Span("description", 40, 45, "angry", "Angry", 8);
        Span span4 = new Span("description", 6, 11, "angry", "angry", 1);

        List<Span> list1 = new ArrayList<>();
        list1.add(span1);
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list1) };
        List<Span> list2 = new ArrayList<>();
        list2.add(span2);
        IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry"), new ListField<>(list2) };

        List<Span> list3 = new ArrayList<>();
        list3.add(span3);
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list3) };

        List<Span> list4 = new ArrayList<>();
        list4.add(span4);
        IField[] fields4 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("Short angry"), new ListField<>(list4) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        ITuple tuple3 = new DataTuple(new Schema(schemaAttributes), fields3);
        ITuple tuple4 = new DataTuple(new Schema(schemaAttributes), fields4);

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        resultList = Utils.removePayload(resultList);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 2);
        Assert.assertTrue(expectedList.containsAll(resultList));
    }
}
