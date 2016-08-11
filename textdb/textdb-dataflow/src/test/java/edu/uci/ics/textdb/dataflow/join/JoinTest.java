package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;
import junit.framework.Assert;

/**
 * 
 * @author sripadks
 *
 */
public class JoinTest {
	private KeywordMatcher keywordMatcherOuter;
	private KeywordMatcher keywordMatcherInner;
	private IDataWriter dataWriterForOuter;
	private DataStore dataStoreForOuter;
	private IDataWriter dataWriterForInner;
	private DataStore dataStoreForInner;
	private Analyzer analyzer;
	private Join join;
	List<ITuple> bookTuple1;
	List<ITuple> bookTuple2;
	List<Attribute> attributeList;
	List<Attribute> modifiedAttributeList;

	// This method sets up some stuff before beginning each test.
	@Before
	public void setup() throws Exception {
		analyzer = new StandardAnalyzer();

		final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
		final Attribute authorAttr = new Attribute("author", FieldType.STRING);
		final Attribute titleAttr = new Attribute("title", FieldType.STRING);
		final Attribute pagesAttr = new Attribute("numberOfPages",
				FieldType.INTEGER);
		final Attribute reviewAttr = new Attribute("reviewOfBook",
				FieldType.TEXT);

		final Attribute[] bookAttr = { idAttr, authorAttr, titleAttr, pagesAttr,
				reviewAttr };
		final Schema bookSchema = new Schema(bookAttr);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task.") };

		IField[] book2 = { new IntegerField(62),
				new StringField("Siddhartha Mukherjee"),
				new StringField("The Gene: An Intimate History"),
				new IntegerField(608),
				new TextField("In 2010, Siddhartha Mukherjee was awarded the "
						+ "Pulitzer Prize for his book The Emperor of All "
						+ "Maladies, a “biography” of cancer.") };

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

		dataStoreForOuter = new DataStore(
				DataConstants.INDEX_DIR + "/join_test_dir_1", bookSchema);
		dataWriterForOuter = new DataWriter(dataStoreForOuter, analyzer);
		dataStoreForInner = new DataStore(
				DataConstants.INDEX_DIR + "/join_test_dir_2", bookSchema);
		dataWriterForInner = new DataWriter(dataStoreForInner, analyzer);
		dataWriterForOuter.clearData();
		dataWriterForInner.clearData();
	}

	// A helper method to get join result. Called from each test case
	public List<ITuple> getJoinResults(IOperator outer, IOperator inner,
			Attribute idAttribute, Attribute joinAttribute, Integer threshold)
					throws Exception {
		JoinPredicate joinPredicate = new JoinPredicate(idAttribute, joinAttribute,
				threshold);
		join = new Join(outer, inner, joinPredicate);
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
	public void writeTuples(List<ITuple> outerTuple, List<ITuple> innerTuple)
			throws Exception {
		if (outerTuple == null) {
			;
		} else {
			dataWriterForOuter.writeData(outerTuple);
		}
		if (innerTuple == null) {
			return;
		}
		dataWriterForInner.writeData(innerTuple);
	}

	// A helper method to setup the test cases.
	// Types allowed (as of now) are:
	// index -> CONJUNCTION_INDEXBASED KeywordMatcher
	// phrase -> PHRASE_INDEXBASED KeywordMatcher
	//
	// whichOperator is to specify either "outer" or "inner" operator.
	public IOperator setupOperators(String query, String type,
			String whichOperator) throws DataFlowException {
		KeywordPredicate keywordPredicate = null;
		IDataStore dataStore = null;
		switch (type) {
		case "index":
			if (whichOperator == "outer") {
			    dataStore = dataStoreForOuter;
				keywordPredicate = new KeywordPredicate(query, modifiedAttributeList,
						analyzer, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
			} else if (whichOperator == "inner") {
	             dataStore = dataStoreForInner;
				keywordPredicate = new KeywordPredicate(query, modifiedAttributeList,
						analyzer, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
			}
			break;
		case "phrase":
			if (whichOperator == "outer") {
	             dataStore = dataStoreForOuter;
				keywordPredicate = new KeywordPredicate(query, modifiedAttributeList,
						analyzer, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED);
			} else if (whichOperator == "inner") {
	              dataStore = dataStoreForInner;
				keywordPredicate = new KeywordPredicate(query, modifiedAttributeList,
						analyzer, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED);
			}
			break;

		default:
			break;
		}
		
        IndexBasedSourceOperator indexInputOperator = new IndexBasedSourceOperator(keywordPredicate.generateDataReaderPredicate(dataStore));
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        keywordMatcher.setInputOperator(indexInputOperator);
        
		return keywordMatcher;
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
					IField[] book1_5 = { new IntegerField(51),
							new StringField("author unknown"),
							new StringField("typical"), new IntegerField(300),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book1_5);
				} else if (index == 4) {
					IField[] book1_4 = { new IntegerField(52),
							new StringField("Mary Roach"),
							new StringField("Grunt: The Curious Science of "
									+ "Humans at War"),
							new IntegerField(288),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book1_4);
				} else if (index == 3) {
					IField[] book1_3 = { new IntegerField(53),
							new StringField("Noah Hawley"),
							new StringField("Before the Fall"),
							new IntegerField(400),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book1_3);
				} else if (index == 2) {
					IField[] book1_2 = { new IntegerField(54),
							new StringField("Andria Williams"),
							new StringField("The Longest Night: A Novel"),
							new IntegerField(400),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book1_2);
				} else if (index == 1) {
					IField[] book1_1 = { new IntegerField(55),
							new StringField("Matti Friedman"),
							new StringField(
									"Pumpkinflowers: A Soldier's " + "Story"),
							new IntegerField(256),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book1_1);
				}
			}
			break;
		case 2:
			while (index > 0) {
				if (index == 5) {
					IField[] book2_5 = { new IntegerField(61),
							new StringField("book author"),
							new StringField("actually typical"),
							new IntegerField(700),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book2_5);
				} else if (index == 4) {
					IField[] book2_4 = { new IntegerField(62),
							new StringField("Siddhartha Mukherjee"),
							new StringField("The Gene: An Intimate History"),
							new IntegerField(608),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book2_4);
				} else if (index == 3) {
					IField[] book2_3 = { new IntegerField(63),
							new StringField("Paul Kalanithi"),
							new StringField("When Breath Becomes Air"),
							new IntegerField(256),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book2_3);
				} else if (index == 2) {
					IField[] book2_2 = { new IntegerField(64),
							new StringField("Matthew Desmond"),
							new StringField(
									"Evicted: Poverty and Profit in the "
											+ "American City"),
							new IntegerField(432),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
					index--;
					tupleArray[index] = new DataTuple(schema, book2_2);
				} else if (index == 1) {
					IField[] book2_1 = { new IntegerField(65),
							new StringField("Sharon Guskin"),
							new StringField("The Forgetting Time: A Novel"),
							new IntegerField(368),
							new TextField("Review of a Book. This is a typical "
									+ "review. This is a test. A book review "
									+ "test. A test to test queries without "
									+ "actually using actual review. From "
									+ "here onwards, we can pretend this to "
									+ "be actually a review even if it is not "
									+ "your typical book review.") };
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "cancer";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 10);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when the IDs of the documents match,
	// fields to join match and the difference of keyword spans is within
	// the given span threshold.
	// e.g.
	// [<11, 18>]
	// [<27, 33>]
	// threshold = 20
	// [   ]
	//          [  ]
	// <-------->
	//     <-------> (within threshold)
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "writer";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 11, 33, "foo",
				"special kind of " + "writer");
		spanList.add(span1);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task."),
				new ListField<>(spanList) };
		ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes),
				book1);
		List<ITuple> expectedResult = new ArrayList<>(1);
		expectedResult.add(expectedTuple);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

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
	// [   ]
	//             [        ]
	// <--------> (beyond threshold)
	// Test result: An empty list is returned.
	@Test
	public void testIdsMatchFieldsMatchSpanExceedThreshold() throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "topics";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when the IDs match, fields to be joined
	// match, but one of the operators result lists has no span. This can happen
	// when one is using FuzzyTokenMatcher with "isSpanInformationAdded = false"
	// Test result: Join should return an empty list.
	@Test
	public void testOneOfTheOperatorResultContainsNoSpan() throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");

		query = "this writer writes well";
		double thresholdRatio = 0.25;
		boolean isSpanInformationAdded = false;
		FuzzyTokenPredicate fuzzyPredicateInner = new FuzzyTokenPredicate(query, dataStoreForInner, attributeList, analyzer,
				thresholdRatio, isSpanInformationAdded);
		FuzzyTokenMatcher fuzzyMatcherInner = new FuzzyTokenMatcher(fuzzyPredicateInner);

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				fuzzyMatcherInner, idAttr, reviewAttr, 20);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when the IDs match, fields to be joined
	// match, but one of the spans to be joined encompasses the other span
	// and both |(span 1 spanStartIndex) - (span 2 spanStartIndex)|,
	// |(span 1 spanEndIndex) - (span 2 spanEndIndex)| are within threshold.
	// e.g.
	// [<11, 18>]
	// [<3, 33>]
	// threshold = 20
	//         [    ]
	//    [               ]
	//    <---->	<-----> (within threshold)
	// Test result: The bigger span should be returned.
	// [<3, 33>]
	@Test
	public void testOneSpanEncompassesOtherAndDifferenceLessThanThreshold()
			throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "takes a special kind of writer";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "phrase",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 3, 33, "foo",
				"takes a special " + "kind of writer");
		spanList.add(span1);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task."),
				new ListField<>(spanList) };
		ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes),
				book1);
		List<ITuple> expectedResult = new ArrayList<>(1);
		expectedResult.add(expectedTuple);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

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
	//         [    ]
	//    [               ]
	//    <-->	      <---> (beyond threshold)
	// Test result: Join should return an empty list.
	@Test
	public void testOneSpanEncompassesOtherAndDifferenceGreaterThanThreshold()
			throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "takes a special kind of writer";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "phrase",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 10);
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
	// [       ]
	//      [          ]
	// <---->  <-------> (within threshold)
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "phrase",
				"outer");
		query = "tract interesting";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "phrase",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 75, 109, "foo",
				"gastrointestinal " + "tract interesting");
		spanList.add(span1);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task."),
				new ListField<>(spanList) };
		ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes),
				book1);
		List<ITuple> expectedResult = new ArrayList<>(1);
		expectedResult.add(expectedTuple);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

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
	// [       ]
	//      [          ]
	// <-->    <-----> (beyond threshold)
	// Test result: Join should return an empty list.
	@Test
	public void testSpansOverlapAndExceedThreshold() throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "takes a special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "phrase",
				"outer");
		query = "special kind of writer";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "phrase",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 10);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when the IDs match, fields to be joined
	// match, but the spans to be joined are the same, i.e. both the keywords
	// are same.
	// e.g.
	// [<11, 18>]
	// [<11, 18>]
	// threshold = 20 (can be any non-negative number)
	// [     ]
	// [     ]
	// Test result: Join should return same span and key and the value in span
	// should be the same.
	// [<11, 18>]
	@Test
	public void testBothTheSpansAreSame() throws Exception {
		writeTuples(bookTuple1, bookTuple1);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "special";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 20);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 11, 18, "foo", "special");
		spanList.add(span1);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task."),
				new ListField<>(spanList) };
		ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes),
				book1);
		List<ITuple> expectedResult = new ArrayList<>(1);
		expectedResult.add(expectedTuple);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

		Assert.assertEquals(1, resultList.size());
		Assert.assertTrue(contains);
	}

	// This case tests for the scenario when the specified ID field of either/
	// both of the operators' does not exist.
	// Test result: Join should return an empty list.
	@Test
	public void testIDFieldDoesNotExist() throws Exception {
		ArrayList<Attribute> list = new ArrayList<>(attributeList.size());
		list.addAll(attributeList);
		list.remove(0);
		final Attribute idAttribute = new Attribute("newId", FieldType.INTEGER);
		list.add(0, idAttribute);

		final Schema schema = new Schema(
				list.toArray(new Attribute[list.size()]));

		IField[] book = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField("Grunt: The Curious Science of Humans at War"),
				new IntegerField(288),
				new TextField("It takes a special kind "
						+ "of writer to make topics ranging from death to our "
						+ "gastrointestinal tract interesting (sometimes "
						+ "hilariously so), and pop science writer Mary Roach is "
						+ "always up to the task.") };
		ArrayList<ITuple> bookTuple = new ArrayList<>(1);
		bookTuple.add(new DataTuple(schema, book));

		writeTuples(bookTuple1, null);

		// For this test case we have to especially setup a dataStore and
		// not use the one setup globally. This is because we have to
		// supply the new schema.
		DataStore dataStore = new DataStore(
				DataConstants.INDEX_DIR + "/join_test_dir_2", schema);
		IDataWriter dataWriter = new DataWriter(dataStore, analyzer);
		dataWriter.clearData();
		dataWriter.writeData(bookTuple);

		String query = "special";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "kind";
		KeywordPredicate keywordPredicate = new KeywordPredicate(query,
				modifiedAttributeList, analyzer,
				DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED
				);
		
        IndexBasedSourceOperator indexInputOperator = new IndexBasedSourceOperator(keywordPredicate.generateDataReaderPredicate(dataStore));
        keywordMatcherInner = new KeywordMatcher(keywordPredicate);
        keywordMatcherInner.setInputOperator(indexInputOperator);
        
		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 10);
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 12);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when one of the operators' has multiple
	// tuples and the other has a single tuple (ID of one of the tuple's in the
	// list of multiple tuples should match with the ID of the single tuple) and
	// spans are within the threshold.
	// e.g.
	// ID:			1		  2		    3		  4
	// Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
	// ID:					  2
	// Tuple:            [<62, 66>]
	// threshold = 12
	// [      ]   [          ]       [  ]    [     ]
	//                 		   [  ]
	//                         <----->
	//                            <-----> (ID match, within threshold)
	// Test result: Join should result in a list with a single tuple with the
	// matched ID and the corresponding joined spans.
	// Tuple: [<62, 73>]
	@Test
	public void testMultipleTuplesAndSingleTupleSpanWithinThreshold()
			throws Exception {
		bookTuple1 = setupTuplesList(1, 4);
		bookTuple2 = setupTuplesList(1, 1);

		writeTuples(bookTuple1, bookTuple2);

		String query = "review";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 12);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 0, 16, "foo",
				"Review of a " + "Book");
		spanList.add(span1);
		Span span2 = new Span(reviewField, 62, 73, "foo", "book review");
		spanList.add(span2);
		Span span3 = new Span(reviewField, 235, 246, "foo", "book review");
		spanList.add(span3);

		IField[] book1 = { new IntegerField(55),
				new StringField("Matti Friedman"),
				new StringField("Pumpkinflowers: A Soldier's " + "Story"),
				new IntegerField(256),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };
		ITuple expectedTuple = new DataTuple(new Schema(schemaAttributes),
				book1);
		List<ITuple> expectedResult = new ArrayList<>(1);
		expectedResult.add(expectedTuple);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

		Assert.assertEquals(1, resultList.size());
		Assert.assertTrue(contains);
	}

	// This case tests for the scenario when one of the operators' has multiple
	// tuples and the other has a single tuple (ID of one of the tuple's in the
	// list of multiple tuples should match with the ID of the single tuple) and
	// none of the spans are not within threshold.
	// e.g.
	// ID:			1		  2		    3		  4
	// Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
	// ID:					  2
	// Tuple:            [<62, 66>]
	// threshold = 4
	// [      ]   [          ]       [  ]    [     ]
	//                 		   [  ]
	//                         <--->
	//                            <--> (ID match, beyond threshold)
	// Test result: Join should result in an empty list.
	@Test
	public void testMultipleTuplesAndSingleTupleSpanExceedThreshold()
			throws Exception {
		bookTuple1 = setupTuplesList(1, 4);
		bookTuple2 = setupTuplesList(1, 1);

		writeTuples(bookTuple1, bookTuple2);

		String query = "review";
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 4);
		Assert.assertEquals(0, resultList.size());
	}

	// This case tests for the scenario when both the operators' have multiple
	// tuples and some of tuples IDs match and spans are within threshold.
	// e.g.
	// ID:			1		  2		    3		  4
	// Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
	// ID:					  2					  4
	// Tuples:           [<62, 66>]			 [<62, 66>]
	// threshold = 12
	// [      ]   [          ]       [  ]    [     ]
	//     [     ]    [		]  [  ]    [  ]       [		]
	//                         <----->		 <---->
	//                            <-----> 		   <---->(ID match, beyond threshold)
	// Test result: Join should result in a list containing tuples with spans.
	// The number of tuples is equal to the number of tuples with both ID match
	// and span within threshold.
	// [<62, 73>][<62, 73>]
	@Test
	public void testBothOperatorsMultipleTuplesSpanWithinThreshold()
			throws Exception {
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 12);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 0, 16, "foo",
				"Review of a " + "Book");
		spanList.add(span1);
		Span span2 = new Span(reviewField, 62, 73, "foo", "book review");
		spanList.add(span2);
		Span span3 = new Span(reviewField, 235, 246, "foo", "book review");
		spanList.add(span3);

		IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField(
						"Grunt: The Curious Science of " + "Humans at War"),
				new IntegerField(288),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book2 = { new IntegerField(54),
				new StringField("Andria Williams"),
				new StringField("The Longest Night: A Novel"),
				new IntegerField(400),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book3 = { new IntegerField(65),
				new StringField("Sharon Guskin"),
				new StringField("The Forgetting Time: A Novel"),
				new IntegerField(368),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book4 = { new IntegerField(63),
				new StringField("Paul Kalanithi"),
				new StringField("When Breath Becomes Air"),
				new IntegerField(256),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes),
				book1);
		ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes),
				book2);
		ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes),
				book3);
		ITuple expectedTuple4 = new DataTuple(new Schema(schemaAttributes),
				book4);
		List<ITuple> expectedResult = new ArrayList<>(4);
		expectedResult.add(expectedTuple1);
		expectedResult.add(expectedTuple2);
		expectedResult.add(expectedTuple3);
		expectedResult.add(expectedTuple4);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

		Assert.assertEquals(4, resultList.size());
		Assert.assertTrue(contains);
	}

	// This case tests for the scenario when both the operators' have multiple
	// tuples and some of tuples IDs match, but none of spans are within
	// threshold.
	// e.g.
	// ID:			1		  2		    3		  4
	// Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
	// ID:					  2					  4
	// Tuples:           [<62, 66>]			 [<62, 66>]
	// threshold = 4
	// [      ]   [          ]       [  ]    [     ]
	//     [     ]    [		]  [  ]    [  ]       [		]
	//                         <----->		 <---->
	//                            <-----> 		   <---->(ID match, beyond threshold)
	// Test result: Join should result in an empty list.
	@Test
	public void testBothOperatorsMultipleTuplesSpanExceedThreshold()
			throws Exception {
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "book";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 4);
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
		keywordMatcherOuter = (KeywordMatcher) setupOperators(query, "index",
				"outer");
		query = "actually";
		keywordMatcherInner = (KeywordMatcher) setupOperators(query, "index",
				"inner");

		Attribute idAttr = attributeList.get(0);
		Attribute reviewAttr = attributeList.get(4);
		List<ITuple> resultList = getJoinResults(keywordMatcherOuter,
				keywordMatcherInner, idAttr, reviewAttr, 90);

		Attribute[] schemaAttributes = new Attribute[attributeList.size() + 1];
		for (int index = 0; index < schemaAttributes.length - 1; index++) {
			schemaAttributes[index] = attributeList.get(index);
		}
		schemaAttributes[schemaAttributes.length
		                 - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

		List<Span> spanList = new ArrayList<>();
		String reviewField = attributeList.get(4).getFieldName();
		// The "foo" (key) is a tentative key; will be replaced by an actual
		// key once implementation is fixed.
		Span span1 = new Span(reviewField, 28, 119, "foo",
				"typical review. " + "This is a test. A book review test. "
						+ "A test to test queries without actually");
		spanList.add(span1);
		Span span2 = new Span(reviewField, 186, 234, "foo",
				"actually a review " + "even if it is not your typical");
		spanList.add(span2);

		IField[] book1 = { new IntegerField(51),
				new StringField("author unknown"), new StringField("typical"),
				new IntegerField(300),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book2 = { new IntegerField(52), new StringField("Mary Roach"),
				new StringField(
						"Grunt: The Curious Science of " + "Humans at War"),
				new IntegerField(288),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book3 = { new IntegerField(53), new StringField("Noah Hawley"),
				new StringField("Before the Fall"), new IntegerField(400),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book4 = { new IntegerField(54),
				new StringField("Andria Williams"),
				new StringField("The Longest Night: A Novel"),
				new IntegerField(400),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		IField[] book5 = { new IntegerField(55),
				new StringField("Matti Friedman"),
				new StringField("Pumpkinflowers: A Soldier's " + "Story"),
				new IntegerField(256),
				new TextField("Review of a Book. This is a typical "
						+ "review. This is a test. A book review "
						+ "test. A test to test queries without "
						+ "actually using actual review. From "
						+ "here onwards, we can pretend this to "
						+ "be actually a review even if it is not "
						+ "your typical book review."),
				new ListField<>(spanList) };

		ITuple expectedTuple1 = new DataTuple(new Schema(schemaAttributes),
				book1);
		ITuple expectedTuple2 = new DataTuple(new Schema(schemaAttributes),
				book2);
		ITuple expectedTuple3 = new DataTuple(new Schema(schemaAttributes),
				book3);
		ITuple expectedTuple4 = new DataTuple(new Schema(schemaAttributes),
				book4);
		ITuple expectedTuple5 = new DataTuple(new Schema(schemaAttributes),
				book5);
		List<ITuple> expectedResult = new ArrayList<>(5);
		expectedResult.add(expectedTuple1);
		expectedResult.add(expectedTuple2);
		expectedResult.add(expectedTuple3);
		expectedResult.add(expectedTuple4);
		expectedResult.add(expectedTuple5);

		boolean contains = TestUtils.containsAllResults(expectedResult,
				resultList);

		Assert.assertEquals(5, resultList.size());
		Assert.assertTrue(contains);
	}
}