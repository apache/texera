package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsChinese;
import edu.uci.ics.texera.api.exception.TexeraException;
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

import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Prakul
 * @author Zuozhi Wang
 * @author Qinhua Huang
 *
 */
public class KeywordConjunctionTest {

    public static final String PEOPLE_TABLE = KeywordTestHelper.PEOPLE_TABLE;
    public static final String MEDLINE_TABLE = KeywordTestHelper.MEDLINE_TABLE;
    public static final String CHINESE_TABLE = KeywordTestHelper.CHINESE_TABLE;
    
    public static final KeywordMatchingType conjunction = KeywordMatchingType.CONJUNCTION_INDEXBASED;
    public static final String RESULTS = KeywordTestHelper.RESULTS;
    
    @BeforeClass
    public static void setUp() throws Exception {
        KeywordTestHelper.writeTestTables();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        KeywordTestHelper.deleteTestTables();
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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Perform the query
        List<Tuple> results = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 0, 4, "tall", "Tall", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];

        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }

        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<>(list) };

        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);

        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

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
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
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
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction);

        // Check the results
        Assert.assertEquals(0, resultList.size());

    }

    @Test
    public void testMatchingWithLimit() throws TexeraException, ParseException, java.text.ParseException {
        String query = "angry";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction, 3, 0);
        List<Tuple> expectedList = new ArrayList<>();

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

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 3);
        Assert.assertTrue(TestUtils.containsAll(expectedList, resultList));
    }

    @Test
    public void testMatchingWithLimitOffset() throws TexeraException, ParseException, java.text.ParseException {
        String query = "angry";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        List<Tuple> resultList = KeywordTestHelper.getQueryResults(PEOPLE_TABLE, query, attributeNames, conjunction, 3, 2);
        List<Tuple> expectedList = new ArrayList<>();

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

        expectedList.add(tuple1);
        expectedList.add(tuple2);
        expectedList.add(tuple3);
        expectedList.add(tuple4);

        Assert.assertEquals(expectedList.size(), 4);
        Assert.assertEquals(resultList.size(), 2);
        Assert.assertTrue(TestUtils.containsAll(expectedList, resultList));
    }
    
    
    /**
     * Verifies GetNextTuple of Keyword Matcher and single word queries in Text
     * Field using Chinese.
     * 
     * @throws Exception
     */
    @Test
    public void testSingleWordQueryInTextFieldChinese() throws Exception {
        // Prepare the query
        String query = "北京大学";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstantsChinese.FIRST_NAME);
        attributeNames.add(TestConstantsChinese.LAST_NAME);
        attributeNames.add(TestConstantsChinese.DESCRIPTION);
        
     // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span = new Span("description", 0, 4, "北京大学", "北京大学", 0);
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstantsChinese.ATTRIBUTES_PEOPLE.length + 1];

        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstantsChinese.ATTRIBUTES_PEOPLE[count];
        }

        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("北京大学电气工程学院"), new ListField<>(list) };

        IField[] fields2 = { new StringField("孔明"), new StringField("洛克贝尔"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("北京大学计算机学院"),
                new ListField<>(list) };
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        Tuple tuple2 = new Tuple(new Schema(schemaAttributes), fields2);

        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(CHINESE_TABLE, query, attributeNames, 
                conjunction, Integer.MAX_VALUE, 0);
        
        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }
    
    
    /**
     * Verifies: data source has multiple attributes, and an entity can appear
     * in all the fields and multiple times.
     * Test for Chinese data.
     * 
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQueryChinese() throws Exception {
        // Prepare the query
        String query = "建筑";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstantsChinese.FIRST_NAME);
        attributeNames.add(TestConstantsChinese.LAST_NAME);
        attributeNames.add(TestConstantsChinese.DESCRIPTION);

        // Prepare the expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("lastName", 0, 2, "建筑", "建筑");
        Span span2 = new Span("description", 3, 5, "建筑", "建筑", 2);
        list.add(span1);
        list.add(span2);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = new Attribute(RESULTS, AttributeType.LIST);

        IField[] fields1 = { new StringField("宋江"), new StringField("建筑"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), 
                new TextField("伟大的建筑是历史的坐标，具有传承的价值。"), new ListField<>(list)};
        
        Tuple tuple1 = new Tuple(new Schema(schemaAttributes), fields1);
        List<Tuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform the query
        List<Tuple> resultList = KeywordTestHelper.getQueryResults(CHINESE_TABLE, query, attributeNames, 
                conjunction, Integer.MAX_VALUE, 0);

        // check the results
        boolean contains = TestUtils.equals(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

}
