package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;
import junit.framework.Assert;

/**
 * 
 * @author sripadks
 *
 */
public class JoinTest {
    private KeywordMatcherSourceOperator keywordSourceOuter;
    private KeywordMatcherSourceOperator keywordSourceInner;
    private DataWriter dataWriterForOuter;
    private DataStore dataStoreForOuter;
    private DataWriter dataWriterForInner;
    private DataStore dataStoreForInner;
    private Analyzer analyzer;
    private Join join;
    List<ITuple> bookTuple1;
    List<ITuple> bookTuple2;
    List<Attribute> attributeList;
    List<Attribute> modifiedAttributeList;
    private int maxVal = Integer.MAX_VALUE;

    // This method sets up some stuff before beginning each test.
    @Before
    public void setup() throws Exception {
        analyzer = new StandardAnalyzer();

        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr = new Attribute("title", FieldType.STRING);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);

        final Attribute[] bookAttr = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr };
        final Schema bookSchema = new Schema(bookAttr);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(62), new StringField("Siddhartha Mukherjee"),
                new StringField("The Gene: An Intimate History"), new IntegerField(608),
                new TextField("In 2010, Siddhartha Mukherjee was awarded the "
                        + "Pulitzer Prize for his book The Emperor of All " + "Maladies, a “biography” of cancer.") };

        attributeList = Arrays.asList(bookAttr);
        ArrayList<Attribute> attLis = new ArrayList<>();
        attLis.addAll(Arrays.asList(bookAttr));
        attLis.remove(idAttr);
        attLis.remove(pagesAttr);
        modifiedAttributeList = (List<Attribute>) attLis;

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForInner.clearData();
    }

    // A helper method to get join result. Called from each test case
    public List<ITuple> getJoinResults(IOperator outer, IOperator inner, Attribute idAttribute, Attribute joinAttribute,
            Integer threshold, int limit, int offset) throws Exception {
        IJoinPredicate joinDistancePredicate = new JoinDistancePredicate(
                idAttribute.getFieldName(), joinAttribute.getFieldName(), threshold);
        join = new Join(outer, inner, joinDistancePredicate);
        join.setLimit(limit);
        join.setOffset(offset);
        join.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        while ((nextTuple = join.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        join.close();
        return results;
    }

    // A helper method to write tuples. Called from each test case
    public void writeTuples(List<ITuple> outerTuple, List<ITuple> innerTuple) throws Exception {
        if (outerTuple == null) {
            ;
        } else {
            dataWriterForOuter.open();
            for (ITuple tuple : outerTuple) {
                dataWriterForOuter.insertTuple(tuple);
            }
            dataWriterForOuter.close();
        }
        if (innerTuple == null) {
            return;
        }
        dataWriterForInner.open();
        for (ITuple tuple : innerTuple) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();
    }

    // A helper method to setup the test cases.
    // Types allowed (as of now) are:
    // index -> CONJUNCTION_INDEXBASED KeywordMatcher
    // phrase -> PHRASE_INDEXBASED KeywordMatcher
    //
    // whichOperator is to specify either "outer" or "inner" operator.
    public IOperator setupOperators(String query, String type, String whichOperator) throws DataFlowException {
        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        switch (type) {
        case "index":
            if (whichOperator == "outer") {
                dataStore = dataStoreForOuter;
                keywordPredicate = new KeywordPredicate(query, 
                        Utils.getAttributeNames(modifiedAttributeList), analyzer,
                        DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
            } else if (whichOperator == "inner") {
                dataStore = dataStoreForInner;
                keywordPredicate = new KeywordPredicate(query, 
                        Utils.getAttributeNames(modifiedAttributeList), analyzer,
                        DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
            }
            break;
        case "phrase":
            if (whichOperator == "outer") {
                dataStore = dataStoreForOuter;
                keywordPredicate = new KeywordPredicate(query, 
                        Utils.getAttributeNames(modifiedAttributeList), analyzer,
                        DataConstants.KeywordMatchingType.PHRASE_INDEXBASED);
            } else if (whichOperator == "inner") {
                dataStore = dataStoreForInner;
                keywordPredicate = new KeywordPredicate(query, 
                        Utils.getAttributeNames(modifiedAttributeList), analyzer,
                        DataConstants.KeywordMatchingType.PHRASE_INDEXBASED);
            }
            break;

        default:
            break;
        }

        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        return keywordSource;
    }

    // A helper method to populate tuples' list to query upon. Currently
    // consists of two sets/lists of tuples with five tuples in each.
    // Takes in the set number and number of tuples.
    public List<ITuple> setupTuplesList(int whichList, int numberOfTuples) {
        int index = numberOfTuples;
        ITuple[] tupleArray;
        if (whichList <= 0 || whichList > 2 || index <= 0 || index > 5) {
            return Arrays.asList(tupleArray = new ITuple[0]);
        }
        tupleArray = new ITuple[index];
        Attribute[] bookAttr = new Attribute[attributeList.size()];
        attributeList.toArray(bookAttr);
        Schema schema = new Schema(bookAttr);

        switch (whichList) {
        case 1:
            while (index > 0) {
                if (index == 5) {
                    IField[] book1_5 = { new IntegerField(51), new StringField("author unknown"),
                            new StringField("typical"), new IntegerField(300),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book1_5);
                } else if (index == 4) {
                    IField[] book1_4 = { new IntegerField(52), new StringField("Mary Roach"),
                            new StringField("Grunt: The Curious Science of " + "Humans at War"), new IntegerField(288),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book1_4);
                } else if (index == 3) {
                    IField[] book1_3 = { new IntegerField(53), new StringField("Noah Hawley"),
                            new StringField("Before the Fall"), new IntegerField(400),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book1_3);
                } else if (index == 2) {
                    IField[] book1_2 = { new IntegerField(54), new StringField("Andria Williams"),
                            new StringField("The Longest Night: A Novel"), new IntegerField(400),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book1_2);
                } else if (index == 1) {
                    IField[] book1_1 = { new IntegerField(55), new StringField("Matti Friedman"),
                            new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book1_1);
                }
            }
            break;
        case 2:
            while (index > 0) {
                if (index == 5) {
                    IField[] book2_5 = { new IntegerField(61), new StringField("book author"),
                            new StringField("actually typical"), new IntegerField(700),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book2_5);
                } else if (index == 4) {
                    IField[] book2_4 = { new IntegerField(62), new StringField("Siddhartha Mukherjee"),
                            new StringField("The Gene: An Intimate History"), new IntegerField(608),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book2_4);
                } else if (index == 3) {
                    IField[] book2_3 = { new IntegerField(63), new StringField("Paul Kalanithi"),
                            new StringField("When Breath Becomes Air"), new IntegerField(256),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book2_3);
                } else if (index == 2) {
                    IField[] book2_2 = { new IntegerField(64), new StringField("Matthew Desmond"),
                            new StringField("Evicted: Poverty and Profit in the " + "American City"),
                            new IntegerField(432),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book2_2);
                } else if (index == 1) {
                    IField[] book2_1 = { new IntegerField(65), new StringField("Sharon Guskin"),
                            new StringField("The Forgetting Time: A Novel"), new IntegerField(368),
                            new TextField("Review of a Book. This is a typical "
                                    + "review. This is a test. A book review " + "test. A test to test queries without "
                                    + "actually using actual review. From " + "here onwards, we can pretend this to "
                                    + "be actually a review even if it is not " + "your typical book review.") };
                    index--;
                    tupleArray[index] = new DataTuple(schema, book2_1);
                }
            }
            break;

        default:
            break;
        }

        return Arrays.asList(tupleArray);
    }

    // This method cleans up after each test.
    @After
    public void cleanUp() throws Exception {
        dataWriterForOuter.clearData();
        dataWriterForInner.clearData();
    }

    // This case tests for scenario when the IDs of the documents don't match.
    // Test result: The list of result returned is empty.
    @Test
    public void testIdsDontMatch() throws Exception {
        writeTuples(bookTuple1, bookTuple2);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "cancer";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 10, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when the IDs of the documents match,
    // fields to join match and the difference of keyword spans is within
    // the given span threshold.
    // e.g.
    // [<11, 18>]
    // [<27, 33>]
    // threshold = 20
    // [           ]
    //          [          ]
    // <-------->
    //             <-------> (within threshold)
    // Test result: The list contains a tuple with all the fields and a span
    // list consisting of the joined span. The joined span is made up of the
    // field name, start and stop index (computed as <min(span1 spanStartIndex,
    // span2 spanStartIndex), max(span1 spanEndIndex, span2 spanEndIndex)>)
    // key (combination of span1 key and span2 key) and value (combination of
    // span1 value and span2 value).
    // [<11, 33>]
    @Test
    public void testIdsMatchFieldsMatchSpanWithinThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "writer";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when the IDs match, fields to join match
    // but the difference of keyword spans to be joined is greater than the
    // threshold.
    // e.g.
    // [<11, 18>]
    // [<42, 48>]
    // threshold = 20
    // [           ]
    //           [     ]
    // <--------> (beyond threshold)
    // Test result: An empty list is returned.
    @Test
    public void testIdsMatchFieldsMatchSpanExceedThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "topics";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when either/both of the operators'
    // result lists are empty (i.e. when one/both of the operators' are
    // not able to find any suitable matches)
    // Test result: Join should return an empty list.
    @Test
    public void testOneOfTheOperatorResultIsEmpty() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the operators result lists has no span. 
    // If one of the operators doesn't have span, then an exception will be thrown.
    // Test result: DataFlowException is thrown
    @Test(expected = DataFlowException.class)
    public void testOneOfTheOperatorResultContainsNoSpan() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");

        query = "this writer writes well";
        double thresholdRatio = 0.25;
        List<String> textAttributeNames = attributeList.stream()
                .filter(attr -> attr.getFieldType() != FieldType.TEXT)
                .map(attr -> attr.getFieldName()).collect(Collectors.toList());
        FuzzyTokenPredicate fuzzyPredicateInner = new FuzzyTokenPredicate(query, textAttributeNames,
                analyzer, thresholdRatio);
        FuzzyTokenMatcher fuzzyMatcherInner = new FuzzyTokenMatcher(fuzzyPredicateInner);
        fuzzyMatcherInner.setInputOperator(new IndexBasedSourceOperator(fuzzyPredicateInner.getDataReaderPredicate(dataStoreForInner)));

        ProjectionPredicate removeSpanListPredicate = new ProjectionPredicate(
                Utils.getAttributeNames(dataStoreForInner.getSchema().getAttributes()));
        ProjectionOperator removeSpanListProjection = new ProjectionOperator(removeSpanListPredicate);
        removeSpanListProjection.setInputOperator(fuzzyMatcherInner);
        
        
        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, removeSpanListProjection, idAttr, reviewAttr, 20, maxVal, 0);
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the spans to be joined encompasses the other span
    // and both |(span 1 spanStartIndex) - (span 2 spanStartIndex)|,
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| are within threshold.
    // e.g.
    // [<11, 18>]
    // [<3, 33>]
    // threshold = 20
    // [              ]
    //      [   ]
    // <---->   <-----> (within threshold)
    // Test result: The bigger span should be returned.
    // [<3, 33>]
    @Test
    public void testOneSpanEncompassesOtherAndDifferenceLessThanThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "takes a special kind of writer";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 3, 33, "special_takes a special " + "kind of writer",
                "takes a special " + "kind of writer");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the spans to be joined encompasses the other span
    // and |(span 1 spanStartIndex) - (span 2 spanStartIndex)|
    // and/or |(span 1 spanEndIndex) - (span 2 spanEndIndex)| exceed threshold.
    // e.g.
    // [<11, 18>]
    // [<3, 33>]
    // threshold = 20
    // [             ]
    //     [    ]
    // <-->     <---> (beyond threshold)
    // Test result: Join should return an empty list.
    @Test
    public void testOneSpanEncompassesOtherAndDifferenceGreaterThanThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "takes a special kind of writer";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 10, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined have some overlap and both
    // |(span 1 spanStartIndex) - (span 2 spanStartIndex)|,
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| are within threshold.
    // e.g.
    // [<75, 97>]
    // [<92, 109>]
    // threshold = 20
    // [      ]
    //      [         ]
    // <----> <-------> (within threshold)
    // Test result: The list contains a tuple with all the fields and a span
    // list consisting of the joined span. The joined span is made up of the
    // field name, start and stop index (computed as <min(span1 spanStartIndex,
    // span2 spanStartIndex), max(span1 spanEndIndex, span2 spanEndIndex)>)
    // key (combination of span1 key and span2 key) and value (combination of
    // span1 value and span2 value).
    // [<75, 109>]
    @Test
    public void testSpansOverlapAndWithinThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "gastrointestinal tract";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "outer");
        query = "tract interesting";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 75, 109, "gastrointestinal tract_" + "tract interesting",
                "gastrointestinal " + "tract interesting");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined have some overlap and
    // |(span 1 spanStartIndex) - (span 2 spanStartIndex)| and/or
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| exceed threshold.
    // e.g.
    // [<75, 97>]
    // [<92, 109>]
    // threshold = 10
    // [    ]
    //    [        ]
    // <--> <-----> (beyond threshold)
    // Test result: Join should return an empty list.
    @Test
    public void testSpansOverlapAndExceedThreshold() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "takes a special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "outer");
        query = "special kind of writer";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "phrase", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 10, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined are the same, i.e. both the keywords
    // are same.
    // e.g.
    // [<11, 18>]
    // [<11, 18>]
    // threshold = 20 (can be any non-negative number)
    // [ ]
    // [ ]
    // Test result: Join should return same span and key and the value in span
    // should be the same.
    // [<11, 18>]
    @Test
    public void testBothTheSpansAreSame() throws Exception {
        writeTuples(bookTuple1, bookTuple1);

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "special";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 11, 18, "special_special", "special");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when the specified ID field of either/
    // both of the operators' does not exist.
    // In this case, an exception is thrown to warn user that join can't be performed without ID field.
    // Test result: DataFlowException is thrown
    @Test(expected = DataFlowException.class)
    public void testIDFieldDoesNotExist() throws Exception {
        ArrayList<Attribute> list = new ArrayList<>(attributeList.size());
        list.addAll(attributeList);
        list.remove(0);
        final Attribute idAttribute = new Attribute("newId", FieldType.INTEGER);
        list.add(0, idAttribute);

        final Schema schema = new Schema(list.toArray(new Attribute[list.size()]));

        IField[] book = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };
        ArrayList<ITuple> bookTuple = new ArrayList<>(1);
        bookTuple.add(new DataTuple(schema, book));

        writeTuples(bookTuple1, null);

        // For this test case we have to especially setup a dataStore and
        // not use the one setup globally. This is because we have to
        // supply the new schema.
        DataStore dataStore = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", schema);
        DataWriter dataWriter = new DataWriter(dataStore, analyzer);
        
        dataWriter.clearData();
        dataWriter.open();
        for (ITuple tuple : bookTuple) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();

        String query = "special";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "kind";
        KeywordPredicate keywordPredicate = new KeywordPredicate(query, Utils.getAttributeNames(modifiedAttributeList), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 10, maxVal, 0);
    }

    // -----------------<Test cases for intersection of tuples>----------------

    // This case tests for the scenario when the tuples have different sets of
    // attributes (hence different schemas) barring the attribute join has to
    // be performed upon (for this case, threshold condition is satisfied).
    // e.g. Schema1: {ID, Author, Pages, Review}
    //		Schema2: {ID, Title, Pages, Review}
    // 		Join Attribute: Review
    // Test result: Join should result in a list with a single tuple which has
    // the attributes common to both the tuples and the joined span.
    @Test
    public void testAttributesAndFieldsIntersection() throws Exception {
        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr = new Attribute("title", FieldType.STRING);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);

        final Attribute[] bookAttr1 = { idAttr, authorAttr, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr1 = { authorAttr, reviewAttr };
        final Attribute[] bookAttr2 = { idAttr, titleAttr, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr2 = { titleAttr, reviewAttr };
        final Schema bookSchema1 = new Schema(bookAttr1);
        final Schema bookSchema2 = new Schema(bookAttr2);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(52), new StringField("Grunt: The Curious Science of Humans at War"),
                new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema1, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema2, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema1);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema2);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForOuter.open();
        for (ITuple tuple : bookTuple1) {
            dataWriterForOuter.insertTuple(tuple);
        }
        dataWriterForOuter.close();
        
        dataWriterForInner.clearData();
        dataWriterForInner.open();        
        for (ITuple tuple : bookTuple2) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();

        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        IndexBasedSourceOperator indexInputOperator = null;

        String query = "special";
        dataStore = dataStoreForOuter;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr1), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        keywordSourceOuter = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        query = "writer";
        dataStore = dataStoreForInner;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr2), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = { idAttr, pagesAttr, reviewAttr, SchemaConstants.SPAN_LIST_ATTRIBUTE };

        List<Span> spanList = new ArrayList<>();
        String reviewField = reviewAttr.getFieldName();

        Span span1 = new Span(reviewField, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    
    /*
     * This case tests for the scenario when one of the attributes (not the
     * one join is performed upon) has different field values (assume threshold
     * condition is satisfied).
     * 
     * e.g. Schema1: {ID, Author, Title, Pages, Review}
     *      Values1: { 2,      A,     B,     5,    ABC}
     *      Schema2: {ID, Author, Title, Pages, Review}
     *      Values2: { 2,      A,     C,     5,    ABC}
     * 
     * The values of "Title" field are different, since "title" is not join attribute,
     * JoinOperator remains silent about the difference, and use the innerTuple's value.
     * 
     * Test result: the output value of Title Field is value of innerTuple, in this test case, "Grunt".
     */
    @Test
    public void testWhenAttributeFieldsAreDifferent() throws Exception {
        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr = new Attribute("title", FieldType.STRING);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);

        final Attribute[] bookAttr = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr = { authorAttr, titleAttr, reviewAttr };
        final Schema bookSchema = new Schema(bookAttr);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"), new StringField("Grunt"),
                new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForOuter.open();
        for (ITuple tuple : bookTuple1) {
            dataWriterForOuter.insertTuple(tuple);
        }
        dataWriterForOuter.close();
 
        dataWriterForInner.clearData();
        dataWriterForInner.open();
        for (ITuple tuple : bookTuple2) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();

        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        IndexBasedSourceOperator indexInputOperator = null;

        String query = "special";
        dataStore = dataStoreForOuter;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceOuter = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        query = "writer";
        dataStore = dataStoreForInner;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr,
                SchemaConstants.SPAN_LIST_ATTRIBUTE };

        List<Span> spanList = new ArrayList<>();
        String reviewField = reviewAttr.getFieldName();

        Span span1 = new Span(reviewField, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new StringField("Mary Roach"), new StringField("Grunt"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when one of the attributes (the one
    // join is performed upon) has different field values (assume threshold
    // condition is satisfied).
    // e.g. Schema1: {ID, Author, Title, Pages, Review}
    //		Values1: { 2,      A,     B,     5,    ABC}
    //		Schema2: {ID, Author, Title, Pages, Review}
    //		Values2: { 2,      A,     B,     5,     AB}
    //		Join Attribute: Review
    // Test result: An empty list is returned.
    @Test
    public void testJoinAttributeFieldsAreDifferent() throws Exception {
        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr = new Attribute("title", FieldType.STRING);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);

        final Attribute[] bookAttr = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr = { authorAttr, titleAttr, reviewAttr };
        final Schema bookSchema = new Schema(bookAttr);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task") };

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForOuter.open();
        for (ITuple tuple : bookTuple1) {
            dataWriterForOuter.insertTuple(tuple);
        }
        dataWriterForOuter.close();
        
        dataWriterForInner.clearData();
        dataWriterForInner.open();
        for (ITuple tuple : bookTuple2) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();

        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        IndexBasedSourceOperator indexInputOperator = null;

        String query = "special";
        dataStore = dataStoreForOuter;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        keywordSourceOuter = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        query = "writer";
        dataStore = dataStoreForInner;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);


        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when an attribute (not the one join is
    // performed upon) has same name and same respective field value but has
    // different field types.
    // Test result: The attribute and the respective field in question is
    // dropped from the result tuple.
    @Test
    public void testWhenAttributeOfSameNameAreDifferent() throws Exception {
        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr1 = new Attribute("title", FieldType.STRING);
        final Attribute titleAttr2 = new Attribute("title", FieldType.TEXT);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);

        final Attribute[] bookAttr1 = { idAttr, authorAttr, titleAttr1, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr1 = { authorAttr, titleAttr1, reviewAttr };
        final Schema bookSchema1 = new Schema(bookAttr1);
        final Attribute[] bookAttr2 = { idAttr, authorAttr, titleAttr2, pagesAttr, reviewAttr };
        final Attribute[] modBookAttr2 = { authorAttr, titleAttr2, reviewAttr };
        final Schema bookSchema2 = new Schema(bookAttr2);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema1, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema2, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema1);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema2);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForOuter.open();
        for (ITuple tuple : bookTuple1) {
            dataWriterForOuter.insertTuple(tuple);
        }
        dataWriterForOuter.close();
        
        dataWriterForInner.clearData();
        dataWriterForInner.open();
        for (ITuple tuple : bookTuple2) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();

        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        IndexBasedSourceOperator indexInputOperator = null;

        String query = "special";
        dataStore = dataStoreForOuter;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr1), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        keywordSourceOuter = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        query = "writer";
        dataStore = dataStoreForInner;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr2), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 20, maxVal, 0);

        Attribute[] schemaAttributes = { idAttr, authorAttr, pagesAttr, reviewAttr,
                SchemaConstants.SPAN_LIST_ATTRIBUTE };

        List<Span> spanList = new ArrayList<>();
        String reviewField = reviewAttr.getFieldName();

        Span span1 = new Span(reviewField, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new StringField("Mary Roach"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    /*
     * This case tests for the scenario when an attribute (the one join is
     * performed upon) has same name, but different field types.
     * 
     * Attributes must have same name and type to be considered equal.
     * 
     * In this case, since it's the attribute to be joined is not the same,
     * an exception is thrown to warn user that join cannot be performed.
     * 
     * Test result: DataFlowException is thrown.
     * 
     */
    @Test(expected = DataFlowException.class)
    public void testJoinAttributeOfSameNameHaveDifferentFieldType() throws Exception {
        final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
        final Attribute authorAttr = new Attribute("author", FieldType.STRING);
        final Attribute titleAttr = new Attribute("title", FieldType.STRING);
        final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
        final Attribute reviewAttr1 = new Attribute("reviewOfBook", FieldType.TEXT);
        final Attribute reviewAttr2 = new Attribute("reviewOfBook", FieldType.STRING);

        final Attribute[] bookAttr1 = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr1 };
        final Attribute[] modBookAttr1 = { authorAttr, titleAttr, reviewAttr1 };
        final Schema bookSchema1 = new Schema(bookAttr1);
        final Attribute[] bookAttr2 = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr2 };
        final Attribute[] modBookAttr2 = { authorAttr, titleAttr, reviewAttr2 };
        final Schema bookSchema2 = new Schema(bookAttr2);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.") };

        bookTuple1 = new ArrayList<>(1);
        bookTuple1.add(new DataTuple(bookSchema1, book1));
        bookTuple2 = new ArrayList<>(1);
        bookTuple2.add(new DataTuple(bookSchema2, book2));

        dataStoreForOuter = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema1);
        dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
        dataStoreForInner = new DataStore(DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema2);
        dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
        
        dataWriterForOuter.clearData();
        dataWriterForOuter.open();
        for (ITuple tuple : bookTuple1) {
            dataWriterForOuter.insertTuple(tuple);
        }
        dataWriterForOuter.close();
        
        dataWriterForInner.clearData();
        dataWriterForInner.open();
        for (ITuple tuple : bookTuple2) {
            dataWriterForInner.insertTuple(tuple);
        }
        dataWriterForInner.close();

        KeywordPredicate keywordPredicate = null;
        IDataStore dataStore = null;
        IndexBasedSourceOperator indexInputOperator = null;

        String query = "special";
        dataStore = dataStoreForOuter;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr1), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceOuter = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        query = "writer";
        dataStore = dataStoreForInner;
        keywordPredicate = new KeywordPredicate(query, 
                Utils.getAttributeNames(modBookAttr2), analyzer,
                DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        keywordSourceInner = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);

        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr1, 20, maxVal, 0);

        Assert.assertEquals(0, resultList.size());
    }

    // --------------------<END of single tuple test cases>--------------------

    // This case tests for the scenario when both the operators' have multiple
    // tuples and none of the tuples have same ID (multi-tuple version of the
    // case when IDs don't match).
    // Test result: Join should result in an empty list.
    @Test
    public void testMultiTupleIdsDontMatch() throws Exception {
        bookTuple1 = setupTuplesList(1, 4);
        bookTuple2 = setupTuplesList(2, 4);

        writeTuples(bookTuple1, bookTuple2);

        String query = "review";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 12, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when one of the operators' has multiple
    // tuples and the other has a single tuple (ID of one of the tuple's in the
    // list of multiple tuples should match with the ID of the single tuple) and
    // spans are within the threshold.
    // e.g.
    // ID: 			1 		  2 		3		  4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID: 		   2
    // Tuple: [<62, 66>]
    // threshold = 12
    // [      ] [ ] [ ] [ ]
    //       [      ]
    // <----->
    //        <-----> (ID match, within threshold)
    // Test result: Join should result in a list with a single tuple with the
    // matched ID and the corresponding joined spans.
    // Tuple: [<62, 73>]
    @Test
    public void testMultipleTuplesAndSingleTupleSpanWithinThreshold() throws Exception {
        bookTuple1 = setupTuplesList(1, 4);
        bookTuple2 = setupTuplesList(1, 1);

        writeTuples(bookTuple1, bookTuple2);

        String query = "review";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 12, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 0, 16, "review_book", "Review of a " + "Book");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 62, 73, "review_book", "book review");
        spanList.add(span2);
        Span span3 = new Span(reviewField, 235, 246, "review_book", "book review");
        spanList.add(span3);

        IField[] book1 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when one of the operators' has multiple
    // tuples and the other has a single tuple (ID of one of the tuple's in the
    // list of multiple tuples should match with the ID of the single tuple) and
    // none of the spans are not within threshold.
    // e.g.
    // ID: 			1		  2		    3		  4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID: 		   2
    // Tuple: [<62, 66>]
    // threshold = 4
    // [ ] [ ] [ ] [ ]
    //      [ ]
    // <--->
    // <--> (ID match, beyond threshold)
    // Test result: Join should result in an empty list.
    @Test
    public void testMultipleTuplesAndSingleTupleSpanExceedThreshold() throws Exception {
        bookTuple1 = setupTuplesList(1, 4);
        bookTuple2 = setupTuplesList(1, 1);

        writeTuples(bookTuple1, bookTuple2);

        String query = "review";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 4, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when both the operators' have multiple
    // tuples and some of tuples IDs match and spans are within threshold.
    // e.g.
    // ID: 			1		  2		    3		  4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID: 			2 		   4
    // Tuples: [<62, 66>] [<62, 66>]
    // threshold = 12
    // [       ]            [      ] [ ] [ ]
    //       [      ] [       ] [ ] [ ] [ ]
    // <-----> <---->
    //                <-----> <---->(ID match, within threshold)
    // Test result: Join should result in a list containing tuples with spans.
    // The number of tuples is equal to the number of tuples with both ID match
    // and span within threshold.
    // [<62, 73>][<62, 73>]
    @Test
    public void testBothOperatorsMultipleTuplesSpanWithinThreshold() throws Exception {
        bookTuple1 = new ArrayList<>(4);
        bookTuple2 = new ArrayList<>(4);
        bookTuple1.addAll(setupTuplesList(1, 4));
        bookTuple2.addAll(setupTuplesList(2, 4));

        bookTuple2.addAll(bookTuple1);
        bookTuple1.remove(0);
        bookTuple1.remove(1);
        bookTuple1.add(bookTuple2.get(2));
        bookTuple1.add(bookTuple2.get(0));

        writeTuples(bookTuple1, bookTuple2);
        String query = "review";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 12, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 0, 16, "review_book", "Review of a " + "Book");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 62, 73, "review_book", "book review");
        spanList.add(span2);
        Span span3 = new Span(reviewField, 235, 246, "review_book", "book review");
        spanList.add(span3);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of " + "Humans at War"), new IntegerField(288),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(65), new StringField("Sharon Guskin"),
                new StringField("The Forgetting Time: A Novel"), new IntegerField(368),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book4 = { new IntegerField(63), new StringField("Paul Kalanithi"),
                new StringField("When Breath Becomes Air"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes), book2);
        ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes), book3);
        ITuple expectedTuple4 = new DataTuple(new Schema(schemaAttributes), book4);
        List<ITuple> expectedResult = new ArrayList<>(4);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(4, resultList.size());
        Assert.assertTrue(contains);
    }

    // This case tests for the scenario when both the operators' have multiple
    // tuples and some of tuples IDs match, but none of spans are within
    // threshold.
    // e.g.
    // ID: 			1		  2		    3		  4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID: 			2  		   4
    // Tuples: [<62, 66>] [<62, 66>]
    // threshold = 4
    // [     ]        [      ]       [      ] [ ]
    // [ ] [ ] [       ]      [       ] [ ]
    //         <-----> <---->
    //                        <-----> <---->(ID match, beyond threshold)
    // Test result: Join should result in an empty list.
    @Test
    public void testBothOperatorsMultipleTuplesSpanExceedThreshold() throws Exception {
        bookTuple1 = new ArrayList<>(4);
        bookTuple2 = new ArrayList<>(4);
        bookTuple1.addAll(setupTuplesList(1, 4));
        bookTuple2.addAll(setupTuplesList(2, 4));

        bookTuple2.addAll(bookTuple1);
        bookTuple1.remove(0);
        bookTuple1.remove(1);
        bookTuple1.add(bookTuple2.get(2));
        bookTuple1.add(bookTuple2.get(0));

        writeTuples(bookTuple1, bookTuple2);
        String query = "review";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "book";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 4, maxVal, 0);
        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when the query has results over multiple
    // fields and join has to be performed only on the field mentioned in the
    // attribute.
    // Test result: Join should return only those tuples which satisfy all the
    // constraints.
    @Test
    public void testQueryHasResultsOverMultipleFields() throws Exception {
        bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, maxVal, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of " + "Humans at War"), new IntegerField(288),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(53), new StringField("Noah Hawley"), new StringField("Before the Fall"),
                new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book4 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book5 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes), book2);
        ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes), book3);
        ITuple expectedTuple4 = new DataTuple(new Schema(schemaAttributes), book4);
        ITuple expectedTuple5 = new DataTuple(new Schema(schemaAttributes), book5);
        List<ITuple> expectedResult = new ArrayList<>(5);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);
        expectedResult.add(expectedTuple5);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(5, resultList.size());
        Assert.assertTrue(contains);
    }

    // ---------------------<Limit and offset test cases.>---------------------

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and less than the actual number of results and offset is 0 and join 
     * is performed.
     * Test result: A list of tuples with number of tuples equal to limit.
     */
    @Test
    public void testForLimitWhenLimitIsLesserThanActualNumberOfResults() throws Exception{
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 3, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes), book2);
        ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes), book3);
        List<ITuple> expectedResult = new ArrayList<>(3);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(3, resultList.size());
        Assert.assertTrue(contains);
    }

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and greater than the actual number of results and offset is 0 and join
     * is performed.
     * Test result: A list of tuples with number of tuples equal to the maximum
     * number of tuples operator can generate (which is lesser than limit.)
     */
    @Test
    public void testForLimitWhenLimitIsGreaterThanActualNumberOfResults() throws Exception{
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 10, 0);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of " + "Humans at War"), new IntegerField(288),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(53), new StringField("Noah Hawley"), new StringField("Before the Fall"),
                new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book4 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book5 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes), book2);
        ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes), book3);
        ITuple expectedTuple4 = new DataTuple(new Schema(schemaAttributes), book4);
        ITuple expectedTuple5 = new DataTuple(new Schema(schemaAttributes), book5);
        List<ITuple> expectedResult = new ArrayList<>(5);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);
        expectedResult.add(expectedTuple5);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(5, resultList.size());
        Assert.assertTrue(contains);
    }

    /*
     * This case tests for the scenario when limit is 0 and offset is 0 and 
     * join is performed.
     * Test result: An empty list.
     */
    @Test
    public void testForLimitWhenLimitIsZero() throws Exception{
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 0, 0);
        Assert.assertEquals(0, resultList.size());
    }

    /*
     * This case tests for the scenario when limit is 0 and offset is some 
     * integer greater than 0 and less than the actual number of results and 
     * join is performed.
     * Test result: An empty list.
     */
    @Test
    public void testForLimitWhenLimitIsZeroAndHasOffset() throws Exception{
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 0, 2);
        Assert.assertEquals(0, resultList.size());
    }

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and less than the actual number of results and offset is some integer
     * greater than 0 and less than actual number of results and join is 
     * performed.
     * Test result: A list of tuples with number of tuples equal to limit 
     * starting from the set offset.
     */
    @Test
    public void testForLimitWhenLimitIsLesserThanActualNumberOfResultsAndHasOffset() throws Exception {
        bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 1, 2);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple1);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(contains);
    }

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and greater than the actual number of results and offset is some integer 
     * greater than 0 and less than actual number of results and join is 
     * performed.
     * Test result: A list of tuples with number of tuples equal to the maximum
     * number of tuples the operator can generate starting from the set offset.
     */
    @Test
    public void testForLimitWhenLimitIsGreaterThanActualNumberOfResultsAndHasOffset() throws Exception {
        bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 10, 2);

        Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
        for (int index = 0; index < schemaAttributes.length - 1; index++) {
            schemaAttributes[index] = attributeList.get(index);
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        List<Span> spanList = new ArrayList<>();
        String reviewField = attributeList.get(4).getFieldName();

        Span span1 = new Span(reviewField, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(reviewField, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of " + "Humans at War"), new IntegerField(288),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(53), new StringField("Noah Hawley"), new StringField("Before the Fall"),
                new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes), book1);
        ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes), book2);
        ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes), book3);
        List<ITuple> expectedResult = new ArrayList<>(3);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);

        boolean contains = TestUtils.equals(expectedResult, resultList);

        Assert.assertEquals(3, resultList.size());
        Assert.assertTrue(contains);
    }

    /*
     * This case tests for the scenario when offset is some integer greater 
     * than 0 and greater than the actual number of results and join is 
     * performed.
     * Test result: An empty list.
     */
    @Test
    public void testOffsetGreaterThanNumberOfResults() throws Exception{
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);
        List<ITuple> resultList = getJoinResults(keywordSourceOuter, keywordSourceInner, idAttr, reviewAttr, 90, 1, 10);
        Assert.assertEquals(0, resultList.size());
    }

    // ------------------------<Test cases for cursor.>------------------------
    /*
     * This case tests for the scenario when open and/or close is called twice 
     * and also when getNextTuple() is called when operator is closed.
     * Test result: Opening or closing the operator twice shouldn't result in 
     * any noticeable difference in operation. But, calling getNetTuple() when 
     * operator is closed should throw an exception.
     */
    @Test(expected = DataFlowException.class)
    public void testWhenOpenOrCloseIsCalledTwiceAndTryToGetNextTupleWhenClosed() throws Exception {
    	bookTuple1 = setupTuplesList(1, 5);
        writeTuples(bookTuple1, bookTuple1);

        String query = "typical";
        keywordSourceOuter = (KeywordMatcherSourceOperator) setupOperators(query, "index", "outer");
        query = "actually";
        keywordSourceInner = (KeywordMatcherSourceOperator) setupOperators(query, "index", "inner");

        Attribute idAttr = attributeList.get(0);
        Attribute reviewAttr = attributeList.get(4);

        IJoinPredicate joinDistancePredicate = new JoinDistancePredicate(
                idAttr.getFieldName(), reviewAttr.getFieldName(), 90);
        join = new Join(keywordSourceOuter, keywordSourceInner, joinDistancePredicate);
        
        join.setLimit(2);
        join.setOffset(2);
        join.open();
        join.open();

        List<ITuple> results = new ArrayList<>();
        ITuple nextTuple = null;

        if ((nextTuple = join.getNextTuple()) != null) {
            results.add(nextTuple);
        }

        join.close();
        join.close();

        Assert.assertEquals(1, results.size());
        
        if ((nextTuple = join.getNextTuple()) != null) {
            results.add(nextTuple);
        }
    }
}