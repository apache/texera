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
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author ZhenfengQi
 *
 */

public class SubstringMatcherTest {

    private IDataWriter dataWriter;
    private DataStore dataStore;
    private Analyzer luceneAnalyzer;

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        luceneAnalyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
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

        KeywordPredicate keywordPredicate = new KeywordPredicate(query, attributeList, luceneAnalyzer,
                DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED);
        
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);
        keywordSource.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = keywordSource.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        return results;
    }

    /**
     * Verifies Substring Matcher where Query phrase does not exist in any
     * document.
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringMatcher() throws Exception {
        // Prepare Query
        String query = "brad and angelina";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Perform Query
        List<ITuple> results = getPeopleQueryResults(query, attributeList);

        // Perform Check
        Assert.assertEquals(0, results.size());
    }

    /**
     * Verifies List<ITuple> returned by Substring Matcher on query with
     * multiple spaces and stop words on a String Field
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringForStringField() throws Exception {
        // Prepare Query
        String query = "short and lin";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("description", 15, 28, "short and lin", "Short and lin");
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

        // Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Verifies List<ITuple> returned multiple results by Substring
     * Matcher on query with spaces on both left side and right side.
     * 
     * @throws Exception
     */
    @Test
    public void testCombinedSpanInMultipleFieldsQuery() throws Exception {
        // Prepare Query
        String query = " lin ";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 24, 29, " lin ", " lin ");

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

        // Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Verifies List<ITuple> returned multiple results by Substring
     * Matcher on query with spaces on both left side and right side.
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringWithStopwordQuery() throws Exception {
        // Prepare Query
        String query = "is";
        ArrayList<Attribute> attributeList = new ArrayList<>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        // Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 12, 14, "is", "is");
        Span span2 = new Span("description", 37, 39, "is", "is");

        list.add(span1);
        list.add(span2);

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

        // Perform Query
        List<ITuple> resultList = getPeopleQueryResults(query, attributeList);

        // Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

}
