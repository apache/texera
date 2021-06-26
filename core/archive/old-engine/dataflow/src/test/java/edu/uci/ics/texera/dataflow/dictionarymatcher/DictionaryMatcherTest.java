package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsChinese;
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
import edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;

/**
 * @author rajeshyarlagadda
 * @author Zuozhi Wang
 * @author Qinhua Huang
 *
 */
public class DictionaryMatcherTest {
    
    public static final String PEOPLE_TABLE = DictionaryMatcherTestHelper.PEOPLE_TABLE;
    public static final String CHINESE_TABLE = DictionaryMatcherTestHelper.CHINESE_TABLE;
    
    public static final String RESULTS = DictionaryMatcherTestHelper.RESULTS;
    public static final Attribute RESULTS_ATTRIBUTE = new Attribute(RESULTS, AttributeType.LIST);

    @BeforeClass
    public static void setUp() throws Exception {
        DictionaryMatcherTestHelper.writeTestTables();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        DictionaryMatcherTestHelper.deleteTestTables();
    }

    /**
     * Scenario S1:verifies GetNextTuple of Dictionary
     * 
     * checking if Dictionary returns all the strings given by the user
     */
    @Test
    public void testGetNextOfDictionaryItem() throws Exception {

        ArrayList<String> expectedStrings = new ArrayList<String>(Arrays.asList("george", "lee", "bruce"));
        Dictionary dictionary = new Dictionary(expectedStrings);
        String dictionaryItem;
        ArrayList<String> returnedStrings = new ArrayList<String>();
        while ((dictionaryItem = dictionary.getNextEntry()) != null) {
            returnedStrings.add(dictionaryItem);
        }
        Assert.assertEquals(expectedStrings, returnedStrings);
    }
    
    /**
     * Scenario: verifies GetNextTuple of Dictionary
     * 
     * Verifies GetNextTuple of Dictionary with all Chinese characters.
     */
    @Test
    public void testGetNextOfDictionaryItemChinese() throws Exception {

        ArrayList<String> expectedStrings = new ArrayList<String>(Arrays.asList("无忌", "孔明", "宋江"));
        Dictionary dictionary = new Dictionary(expectedStrings);
        String dictionaryItem;
        ArrayList<String> returnedStrings = new ArrayList<String>();
        while ((dictionaryItem = dictionary.getNextEntry()) != null) {
            returnedStrings.add(dictionaryItem);
        }
        Assert.assertEquals(expectedStrings, returnedStrings);
    }

    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in String Field using SCANOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("bruce"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in String Field using SUBSTRING_SCANBASED
     * Test in Chinese.
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingScanChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("孔明"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 2, "孔明", "孔明");
        list.add(span);

        Schema resultSchema = new Schema.Builder().add(TestConstantsChinese.SCHEMA_PEOPLE).add(RESULTS_ATTRIBUTE).build();

        IField[] fields1 = { new StringField("孔明"), new StringField("洛克贝尔"), new IntegerField(42),
                new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")),
                new TextField("北京大学计算机学院"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(resultSchema, fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using KEYWORDOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("john Lee", "bruce"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 8, "john Lee", "john Lee");
        Span span2 = new Span("firstName", 0, 5, "bruce", "bruce");
        list1.add(span1);
        list1.add(span2);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);

        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);

        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using KEYWORDOPERATOR
     * Test in Chinese.
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingKeywordChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("无忌", "长孙"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 2, "长孙", "长孙");
        Span span2 = new Span("firstName", 0, 2, "无忌", "无忌");
        list1.add(span1);
        list1.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);

        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);
        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using PHRASEOPERATOR
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("john Lee", "bruce"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 8, "john Lee", "john Lee");
        Span span2 = new Span("firstName", 0, 5, "bruce", "bruce");
        list1.add(span1);
        list1.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);

        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);
        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and multiple word
     * queries in String Field using PHRASE_INDEXBASED in Chinese.
     */

    @Test
    public void testSingleWordQueryInStringFieldUsingPhraseChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("长孙", "无忌"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 2, "长孙", "长孙");
        Span span2 = new Span("firstName", 0, 2, "无忌", "无忌");
        list1.add(span1);
        list1.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);

        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);

        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-5:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using SCANOPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using SCANOPERATOR in Chinese.
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingScanChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("学院"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("description", 8, 10, "学院", "学院");
        list1.add(span1);
        
        List<Span> list2 = new ArrayList<Span>();
        Span span2 = new Span("description", 7, 9, "学院", "学院");
        list2.add(span2);
        
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<Span>(list1) };
        IField[] fields2 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), 
                new TextField("北京大学计算机学院"),
                new ListField<Span>(list2) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using KEYWORD OPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using KEYWORD OPERATOR in Chinese
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingKeywordChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("北京大学"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "北京大学", "北京大学", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("北京大学计算机学院"),
                new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);
        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, 
                attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-7:verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using PHRASE OPERATOR
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies GetNextTuple of DictionaryMatcher and single word
     * queries in Text Field using PHRASE OPERATOR in Chinese.
     */

    @Test
    public void testSingleWordQueryInTextFieldUsingPhraseChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("北京大学"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "北京大学", "北京大学");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("北京大学计算机学院"),
                new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, 
                attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-8:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using SCAN OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using SCAN OPERATOR in Chinese
     */

    @Test
    public void testMultipleWordsQueryUsingScanChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("洛克贝尔"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("lastName", 0, 4, "洛克贝尔", "洛克贝尔");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("北京大学计算机学院"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, 
                dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-9:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using KEYWORD OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingKeyword() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario: verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using KEYWORD OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingKeywordChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("洛克贝尔"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("lastName", 0, 4, "洛克贝尔", "洛克贝尔");;
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), 
                new TextField("北京大学计算机学院"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, 
                attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-10:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using PHRASE OPERATOR
     */

    @Test
    public void testMultipleWordsQueryUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    /**
     * Scenario S-10C:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries using PHRASE OPERATOR in Chinese
     */

    @Test
    public void testMultipleWordsQueryUsingPhraseChinese() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("洛克贝尔"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("lastName", 0, 4, "洛克贝尔", "洛克贝尔");;
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), 
                new TextField("北京大学计算机学院"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstantsChinese.FIRST_NAME, TestConstantsChinese.LAST_NAME,
                TestConstantsChinese.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(CHINESE_TABLE, dictionary, 
                attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-11:verifies: data source has multiple attributes, and an
     * entity can appear in all the fields and multiple times using SUBSTRING_SCANBASE
     * OPERATOR.
     */

    @Test
    public void testWordInMultipleFieldsQueryUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin clooney"));
        Dictionary dictionary = new Dictionary(names);
        // create a data tuple first
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
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);
        
        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
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
        Dictionary dictionary = new Dictionary(names);
        // create a data tuple first
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
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.CONJUNCTION_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
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
        Dictionary dictionary = new Dictionary(names);
        // create a data tuple first
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
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S-14:verifies: Query with Stop Words match corresponding phrases
     * in the document using PHRASE OPERATOR.
     */

    @Test
    public void testStopWordsInQueryUsingPhrase() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin and is angry"));
        Dictionary dictionary = new Dictionary(names);
        // create a data tuple first
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 25, 45, "lin and is angry", "lin clooney is Angry");
        list.add(span);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    public void testMatchingWithLimit() throws Exception {
        ArrayList<String> word = new ArrayList<String>(Arrays.asList("angry"));
        Dictionary dictionary = new Dictionary(word);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

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

        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        Tuple tuple3 = new Tuple(new Schema(schemaAttributes), fields3);
        Tuple tuple4 = new Tuple(new Schema(schemaAttributes), fields4);

        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);
        List<Tuple> expectedList = new ArrayList<>();
        List<Tuple> resultList = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED, 3, 0);

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
        Dictionary dictionary = new Dictionary(word);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

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

        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        Tuple tuple3 = new Tuple(new Schema(schemaAttributes), fields3);
        Tuple tuple4 = new Tuple(new Schema(schemaAttributes), fields4);

        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);
        List<Tuple> expectedList = new ArrayList<>();
        List<Tuple> resultList = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.PHRASE_INDEXBASED, 1, 3);

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 1);
        Assert.assertTrue(TestUtils.containsAll(expectedList, resultList));
    }

    /***
     * Testcases for scan-based DictionaryMatcher, use getScanSourceResults method only.
     * @throws Exception
     */
    @Test
    public void testMultipleWordQueryInTextFieldUsingScan1() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall","fair"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list = new ArrayList<Span>();
        List<Span> list1 = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        Span span1 = new Span("description", 5, 9, "fair","Fair");
        list.add(span);
        list1.add(span);
        list1.add(span1);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    @Test
    public void testMultipleWordQueryInTextFieldUsingScan2() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall fair"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("description", 0, 9, "tall fair","Tall Fair");
        list1.add(span1);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list1) };
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME,
                TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }


    @Test
    public void testMultipleWordQueryInStringFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("christian john wayne", "rock bale"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 20, "christian john wayne","christian john wayne");
        Span span2 = new Span("lastName", 0, 9, "rock bale", "rock bale");
        list1.add(span1);
        list1.add(span2);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    @Test
    public void testMultipleWordQueryInStringandTestFieldUsingScan() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("christian john wayne", "rock bale", "fair"));
        Dictionary dictionary = new Dictionary(names);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 20, "christian john wayne","christian john wayne");
        Span span2 = new Span("lastName", 0, 9, "rock bale", "rock bale");
        Span span3 = new Span("description", 5, 9, "fair","Fair");
        list1.add(span1);
        list1.add(span2);
        list1.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list1) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME, TestConstants.DESCRIPTION);

        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.SUBSTRING_SCANBASED);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    @Test
    public void testRegexQuery() throws Exception {

        ArrayList<String> patterns = new ArrayList<String>(Arrays.asList("\\w+\\sjohn", "Tall\\s*\\w{4,5}"));
        Dictionary dictionary = new Dictionary(patterns);

        // create a data tuple first
        List<Span> list1 = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "\\w+\\sjohn", "christian john");
        Span span2 = new Span("description", 0, 9, "Tall\\s*\\w{4,5}", "Tall Fair");
        list1.add(span1);
        list1.add(span2);

        List<Span> list2 = new ArrayList<>();
        Span span3 = new Span("description", 0, 10, "Tall\\s*\\w{4,5}", "Tall Angry");
        list2.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = RESULTS_ATTRIBUTE;

        IField[] fields1 = {new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list2)};

        IField[] fields2 = {new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list1)};

        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<String> attributeNames = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME, TestConstants.DESCRIPTION);
        List<Tuple> returnedResults = DictionaryMatcherTestHelper.getQueryResults(PEOPLE_TABLE, dictionary, attributeNames, KeywordMatchingType.REGEX);
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

}

