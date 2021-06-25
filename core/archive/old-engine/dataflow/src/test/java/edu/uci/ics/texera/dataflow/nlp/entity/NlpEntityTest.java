package edu.uci.ics.texera.dataflow.nlp.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Feng [sam0227]
 */
public class NlpEntityTest {
    
    public static final String ONE_SENTENCE_TABLE = "nlp_test_two_sentence";
    public static final String TWO_SENTENCE_TABLE = "nlp_test_one_sentence";
    
    public static final String RESULTS = NlpEntityTestConstants.RESULT;
    
    @BeforeClass
    public static void setUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        relationManager.createTable(ONE_SENTENCE_TABLE, TestUtils.getDefaultTestIndex().resolve(ONE_SENTENCE_TABLE), 
                NlpEntityTestConstants.SCHEMA_ONE_SENTENCE, LuceneAnalyzerConstants.standardAnalyzerString());
        relationManager.createTable(TWO_SENTENCE_TABLE, TestUtils.getDefaultTestIndex().resolve(TWO_SENTENCE_TABLE), 
                NlpEntityTestConstants.SCHEMA_TWO_SENTENCE, LuceneAnalyzerConstants.standardAnalyzerString());
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(ONE_SENTENCE_TABLE);
        relationManager.deleteTable(TWO_SENTENCE_TABLE);
    }
    
    // table is cleared after each test case
    @After
    public void deleteData() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        DataWriter oneSentenceDataWriter = relationManager.getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        oneSentenceDataWriter.clearData();
        oneSentenceDataWriter.close();
        
        DataWriter twoSentenceDataWriter = relationManager.getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        twoSentenceDataWriter.clearData();
        twoSentenceDataWriter.close();
    }
    
    /**
     * @param NLP_TEST_TABLE
     * @param attributes
     * @param nlpEntityType
     * @return
     * @throws Exception
     * @about Using nlpEntityOperator to get all returned results from
     *        NLP_TEST_TABLE, return as a list of tuples
     */
    public List<Tuple> getQueryResults(String tableName, List<String> attributeNames,
            NlpEntityType nlpEntityType) throws Exception {
        return getQueryResults(tableName, attributeNames, nlpEntityType, Integer.MAX_VALUE, 0);
    }
    
    public List<Tuple> getQueryResults(String tableName, List<String> attributeNames,
            NlpEntityType nlpEntityType, int limit, int offset) throws Exception {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));

        NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(nlpEntityType, attributeNames, RESULTS);
        NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        nlpEntityOperator.setInputOperator(scanSource);

        nlpEntityOperator.setLimit(limit);
        nlpEntityOperator.setOffset(offset);
        
        Tuple nextTuple = null;
        List<Tuple> results = new ArrayList<Tuple>();
        
        nlpEntityOperator.open();
        while ((nextTuple = nlpEntityOperator.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        nlpEntityOperator.close();
        
        return results;
    }

    /**
     * Scenario 1: Test getNextTuple with only one span in the return list Text
     * : Microsoft is a organization. Search for all NE_ALL entity types
     *
     * @throws Exception
     */
    @Test
    public void getNextTupleTest1() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest1Tuple();
        
        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();

        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);

        List<Tuple> expectedResults = NlpEntityTestConstants.getTest1ResultTuples();
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario 2: Test getNextTuple with more than one span in the return list
     * Text: Microsoft, Google and Facebook are organizations Search for all
     * NE_ALL entity types
     */
    @Test
    public void getNextTupleTest2() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest2Tuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest2ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);

    }

    /**
     * Scenario 3: Test getNextTuple with more than one span in the return list
     * and with different recognized classes. Text: Microsoft, Google and
     * Facebook are organizations and Donald Trump and Barack Obama are persons.
     * Search for all NE_ALL entity types
     */
    @Test
    public void getNextTupleTest3() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest3Tuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest3ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 4:Test getNextTuple with more than one span in the return list
     * and with different recognized classes and more than one fields in the
     * source tuple.
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons. Search for all NE_ALL entity
     * types
     */
    @Test
    public void getNextTupleTest4() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest4Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        String attribute2 = NlpEntityTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest4ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 5:Test getNextTuple using two fields:
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons.
     * <p>
     * Only search the second field for all NE_ALL entity types
     */
    @Test
    public void getNextTupleTest5() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest4Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();
        
        String attribute = NlpEntityTestConstants.SENTENCE_TWO;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);

        List<Tuple> expectedResults = NlpEntityTestConstants.getTest5ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 6:Test getNextTuple using two fields:
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons.
     * <p>
     * Only search for Organization for all fields.
     */
    @Test
    public void getNextTupleTest6() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest4Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();

        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        String attribute2 = NlpEntityTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                NlpEntityType.ORGANIZATION);

        List<Tuple> expectedResults = NlpEntityTestConstants.getTest6ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 7:Test getNextTuple using sentence: Sentence1: Feeling the warm
     * sun rays beaming steadily down, the girl decided there was no need to
     * wear a coat. Search for Adjective.
     */
    @Test
    public void getNextTupleTest7() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest7Tuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.ADJECTIVE);

        List<Tuple> expectedResults = NlpEntityTestConstants.getTest7ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    @Test
    public void getNextTupleTest8() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest8Tuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();

        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.MONEY);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest8ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    @Test
    public void getNextTupleTest9() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTest9Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();

        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        String attribute2 = NlpEntityTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest9ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void getNextTupleTest10() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getOneSentenceTestTuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest10ResultTuples();
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void getNextTupleTest11() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getTwoSentenceTestTuple();
        
        DataWriter twoSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        String attribute2 = NlpEntityTestConstants.SENTENCE_TWO;
        List<String> attributeNames = Arrays.asList(attribute1, attribute2);
        
        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL);
        
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest11ResultTuple();  
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    public void getNextTupleTestWithLimit() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getOneSentenceTestTuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL, 3, 0);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest10ResultTuples();
        
        // ExpectedResults is the array containing all the matches.
        // Since the order of returning records in returnedResults is not deterministic, we use containsAll
        // to ensure that the records in returnedResults are included in the ExpectedResults.
        Assert.assertEquals(returnedResults.size(), 3);
        Assert.assertTrue(TestUtils.containsAll(expectedResults, returnedResults));
    }
    
    public void getNextTupleTestWithLimitOffset() throws Exception {
        List<Tuple> data = NlpEntityTestConstants.getOneSentenceTestTuple();

        DataWriter oneSentenceDataWriter = RelationManager.getInstance().getTableDataWriter(ONE_SENTENCE_TABLE);
        oneSentenceDataWriter.open();
        for (Tuple tuple : data) {
            oneSentenceDataWriter.insertTuple(tuple);
        }
        oneSentenceDataWriter.close();
        
        String attribute1 = NlpEntityTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<Tuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpEntityType.NE_ALL, 2, 2);
        List<Tuple> expectedResults = NlpEntityTestConstants.getTest10ResultTuples();
        
        Assert.assertEquals(returnedResults.size(), 2);
        Assert.assertTrue(TestUtils.containsAll(expectedResults, returnedResults));
    }

}
