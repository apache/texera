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

public class PhraseMatcherTest {

    private KeywordMatcher KeywordMatcher;
    private IDataWriter dataWriter;
    private DataStore dataStore;
    private Analyzer luceneAnalyzer;

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

        IPredicate predicate = new KeywordPredicate(query, attributeList, DataConstants.KeywordOperatorType.PHRASE, luceneAnalyzer, dataStore);
        KeywordMatcher = new KeywordMatcher(predicate);
        KeywordMatcher.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = KeywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }

    /**
     * Verifies Phrase Matcher where Query phrase doesn't exist in any document.
     * @throws Exception
     */
    @Test
    public void testKeywordMatcher() throws Exception {
        //Prepare Query
        String query = "lin clooney is short and angry";
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
     * Verifies List<ITuple> returned by Phrase Matcher on multiple
     * word query on a String Field
     * @throws Exception
     */
    @Test
    public void testPhraseSearchForStringField() throws Exception {
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
     * Verifies: getNextTuple should return Combined Span info for the phrase
     * @throws Exception
     */
    @Test
    public void testCombinedSpanInMultipleFieldsQuery() throws Exception {
        //Prepare Query
        String query = "lin clooney";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 11, "lin clooney", "Lin Clooney");
        Span span3 = new Span("description", 25, 36, "lin clooney", "lin clooney");

        list.add(span1);
        list.add(span2);
        list.add(span3);


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
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords1() throws Exception {
        //Prepare Query
        String query = "lin and and angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 25, 45, "lin and and angry", "lin clooney is Angry");

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
     * Verifies: Query with Stop Words match corresponding phrases in the document
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQueryWithStopWords2() throws Exception {
        //Prepare Query
        String query = "lin clooney and angry";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 25, 45, "lin clooney and angry", "lin clooney is Angry");

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


}
