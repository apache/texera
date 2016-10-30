
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
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
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author rajeshyarlagadda
 *
 */
public class DictionaryMatcherTest {

    private DataStore dataStore;
    private IDataWriter dataWriter;
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

    public List<ITuple> getQueryResults(IDictionary dictionary, KeywordMatchingType srcOpType,
            List<Attribute> attributes) throws Exception {
        return getQueryResults(dictionary, srcOpType, attributes, Integer.MAX_VALUE, 0);
    }

    public List<ITuple> getQueryResults(IDictionary dictionary, KeywordMatchingType srcOpType,
            List<Attribute> attributes, int limit) throws Exception {
        return getQueryResults(dictionary, srcOpType, attributes, limit, 0);
    }

    public List<ITuple> getQueryResults(IDictionary dictionary, KeywordMatchingType srcOpType,
            List<Attribute> attributes, int limit, int offset) throws Exception {
        List<ITuple> dictionaryMatcherSourceOperatorResults = getDictionaryMatcherSourceOperatorResults(dictionary,
                srcOpType, attributes, limit, offset);
        List<ITuple> dictionaryMatcherResults = getDictionaryMatcherResults(dictionary, srcOpType, attributes, limit,
                offset);

        Assert.assertTrue(
                TestUtils.containsAllResults(dictionaryMatcherSourceOperatorResults, dictionaryMatcherResults));

        return dictionaryMatcherResults;
    }

    private List<ITuple> getDictionaryMatcherSourceOperatorResults(IDictionary dictionary,
            KeywordMatchingType srcOpType, List<Attribute> attributes, int limit, int offset) throws Exception {
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, attributes, luceneAnalyzer,
                srcOpType);
        DictionaryMatcherSourceOperator dictionaryMatcher = new DictionaryMatcherSourceOperator(dictionaryPredicate,
                dataStore);

        dictionaryMatcher.open();
        dictionaryMatcher.setLimit(limit);
        dictionaryMatcher.setOffset(offset);
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        dictionaryMatcher.close();
        return results;
    }

    private List<ITuple> getDictionaryMatcherResults(IDictionary dictionary, KeywordMatchingType srcOpType,
            List<Attribute> attributes, int limit, int offset) throws Exception {
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, attributes, luceneAnalyzer,
                srcOpType);

        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
        ScanBasedSourceOperator indexSource = new ScanBasedSourceOperator(dataStore, luceneAnalyzer);
        dictionaryMatcher.setInputOperator(indexSource);

        dictionaryMatcher.open();
        dictionaryMatcher.setLimit(limit);
        dictionaryMatcher.setOffset(offset);
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        dictionaryMatcher.close();
        return results;
    }

    /**
     * Scenario S1:verifies GetNextTuple of Dictionary
     * 
     * checking if Dictionary returns all the strings given by the user
     */

    @Test
    public void testGetNextOfDictionaryItem() throws Exception {

        ArrayList<String> expectedStrings = new ArrayList<String>(Arrays.asList("george", "lee", "bruce"));
        IDictionary dictionary = new Dictionary(expectedStrings);
        String dictionaryItem;
        ArrayList<String> returnedStrings = new ArrayList<String>();
        while ((dictionaryItem = dictionary.getNextValue()) != null) {
            returnedStrings.add(dictionaryItem);
        }
        boolean contains = TestUtils.containsAllResults(expectedStrings, returnedStrings);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-2:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in String Field using SCANOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("bruce"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.SUBSTRING_SCANBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-3:verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using KEYWORDOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("john Lee", "bruce"));
        Dictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list1 = new ArrayList<Span>();
        List<Span> list2 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 8, "john Lee", "john Lee");
        Span span2 = new Span("firstName", 0, 5, "bruce", "bruce");
        list1.add(span1);
        list2.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list1) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        IField[] fields2 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list2) };
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.CONJUNCTION_INDEXBASED,
                attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-4:verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using PHRASEOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("john Lee", "bruce"));
        Dictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list1 = new ArrayList<Span>();
        List<Span> list2 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 8, "john Lee", "john Lee");
        Span span2 = new Span("firstName", 0, 5, "bruce", "bruce");
        list1.add(span1);
        list2.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list1) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        IField[] fields2 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list2) };
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-5:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using SCANOPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.SUBSTRING_SCANBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-6:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using KEYWORD OPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.CONJUNCTION_INDEXBASED,
                attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-7:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using PHRASE OPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-8:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using SCAN OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.SUBSTRING_SCANBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-9:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using KEYWORD OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.CONJUNCTION_INDEXBASED,
                attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-10:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using PHRASE OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        IDictionary dictionary = new Dictionary(names);

        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-11:verifies: data source has multiple attributes, and an
     * entity can appear in all the fields and multiple times using SCAN
     * OPERATOR.
     */

    @Test
    public void testWordInMultipleFieldsQueryUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin clooney"));
        IDictionary dictionary = new Dictionary(names);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 11, "lin clooney", "Lin Clooney");
        Span span3 = new Span("description", 25, 36, "lin clooney", "lin clooney");
        list.add(span1);
        list.add(span2);
        list.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.SUBSTRING_SCANBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-12:verifies: data source has multiple attributes, and an
     * entity can appear in all the fields and multiple times using KEYWORD
     * OPERATOR.
     */

    @Test
    public void testWordInMultipleFieldsQueryUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin clooney"));
        IDictionary dictionary = new Dictionary(names);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();

        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");

        Span span2 = new Span("description", 0, 3, "lin", "Lin", 0);
        Span span3 = new Span("description", 4, 11, "clooney", "Clooney", 1);
        Span span4 = new Span("description", 25, 28, "lin", "lin", 5);
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
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.CONJUNCTION_INDEXBASED,
                attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-13:verifies: data source has multiple attributes, and an
     * entity can appear in all the fields and multiple times using PHRASE
     * OPERATOR.
     */

    @Test
    public void testWordInMultipleFieldsQueryUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin clooney"));
        IDictionary dictionary = new Dictionary(names);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 11, "lin clooney", "Lin Clooney");
        Span span3 = new Span("description", 25, 36, "lin clooney", "lin clooney");
        list.add(span1);
        list.add(span2);
        list.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-14:verifies: Query with Stop Words match corresponding phrases
     * in the document using PHRASE OPERATOR.
     */

    @Test
    public void testStopWordsInQueryUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin and is angry"));
        IDictionary dictionary = new Dictionary(names);
        // create data tuple first
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 25, 45, "lin and is angry", "lin clooney is Angry");
        list.add(span);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    public void testMatchingWithLimit() throws Exception {
        ArrayList<String> word = new ArrayList<String>(Arrays.asList("angry"));
        IDictionary dictionary = new Dictionary(word);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

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

        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);
        List<ITuple> expectedList = new ArrayList<>();
        List<ITuple> resultList = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes, 3);

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 3);
        Assert.assertTrue(expectedList.containsAll(resultList));
    }

    @Test
    public void testMatchingWithLimitOffset() throws Exception {
        ArrayList<String> word = new ArrayList<String>(Arrays.asList("angry"));
        IDictionary dictionary = new Dictionary(word);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        Span span1 = new Span("description", 5, 10, "angry", "Angry");
        Span span2 = new Span("description", 6, 11, "angry", "Angry");
        Span span3 = new Span("description", 40, 45, "angry", "Angry");
        Span span4 = new Span("description", 6, 11, "angry", "angry");

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

        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);
        List<ITuple> expectedList = new ArrayList<>();
        List<ITuple> resultList = getQueryResults(dictionary, KeywordMatchingType.PHRASE_INDEXBASED, attributes, 1, 1);

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        resultList = Utils.removePayload(resultList);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 1);
        Assert.assertTrue(expectedList.containsAll(resultList));
    }
}