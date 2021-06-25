package edu.uci.ics.texera.dataflow.comparablematcher;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsRegexSplit;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sweetest.sj on 10/4/16.
 */
public class ComparableMatcherTest {
    
    public static final String PEOPLE_TABLE = "comparable_test_people";
    public static final String PEOPLE_TABLE_2 = "comparable_test_people_2";

    @BeforeClass
    public static void setUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, TestUtils.getDefaultTestIndex().resolve(PEOPLE_TABLE), 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
        
        // create the people table 2 and write tuples
        relationManager.createTable(PEOPLE_TABLE_2, TestUtils.getDefaultTestIndex().resolve(PEOPLE_TABLE_2), 
                TestConstantsRegexSplit.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter people2DataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE_2);
        people2DataWriter.open();
        for (Tuple tuple : TestConstantsRegexSplit.constructSamplePeopleTuples()) {
            people2DataWriter.insertTuple(tuple);
        }
        people2DataWriter.close();
    }

    @AfterClass
    public static void cleanUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(PEOPLE_TABLE);
        relationManager.deleteTable(PEOPLE_TABLE_2);
    }
    
    public List<Tuple> getQueryResults(String attributeName, ComparisonType matchingType, Object compareToValue)
            throws TexeraException {
        // Perform the query
        ComparablePredicate comparablePredicate = new ComparablePredicate(attributeName, matchingType, compareToValue.toString());
        ComparableMatcher comparableMatcher = new ComparableMatcher(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        comparableMatcher.open();
        List<Tuple> results =  getQueryResults(comparableMatcher);
        comparableMatcher.close();
        return results;
    }

    public void setDefaultMatcherConfig(ComparableMatcher comparableMatcher) throws TexeraException {
        // Perform the query
        ScanBasedSourceOperator sourceOperator = new ScanBasedSourceOperator(
                new ScanSourcePredicate(PEOPLE_TABLE));
        comparableMatcher.setInputOperator(sourceOperator);
        comparableMatcher.setLimit(Integer.MAX_VALUE);
        comparableMatcher.setOffset(0);
    }

    public List<Tuple> getQueryResults(ComparableMatcher comparableMatcher) throws TexeraException {
        List<Tuple> returnedResults = new ArrayList<>();
        Tuple nextTuple = null;

        while ((nextTuple = comparableMatcher.getNextTuple()) != null) {
            returnedResults.add(nextTuple);
        }
        return returnedResults;
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type GREATER_THAN
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching1() throws Exception {
        // Prepare the query
        double threshold = 6.05;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        // check the results
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));

        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type LESS_THAN
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching2() throws Exception {
        // Prepare the query
        double threshold = 5.75;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type LESS_THAN_OR_EQUAL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching3() throws Exception {
        // Prepare the query
        double threshold = 5.95;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN_OR_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type GREATER_THAN_OR_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching4() throws Exception {
        // Prepare the query
        double threshold = 5.95;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN_OR_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);


        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type EQUAL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching5() throws Exception {
        // Prepare the query
        double threshold = 6.10;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Double> with matching type NOT_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDoubleMatching6() throws Exception {
        // Prepare the query
        double threshold = 6.10;
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.NOT_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);


        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type EQUAL_TO
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching1() throws Exception {
        // Prepare the query
        int threshold = 42;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));


        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type GREATER_THAN
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching2() throws Exception {
        // Prepare the query
        int threshold = 45;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));


        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type GREATER_THAN_OR_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching3() throws Exception {
        // Prepare the query
        int threshold = 45;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN_OR_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));


        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type LESS_THAN_OR_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching4() throws Exception {
        // Prepare the query
        int threshold = 43;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN_OR_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));


        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type LESS_THAN
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching5() throws Exception {
        // Prepare the query
        int threshold = 43;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));
        
        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Integer> with matching type NOT_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testIntegerMatching6() throws Exception {
        // Prepare the query
        int threshold = 43;
        Attribute attribute = TestConstants.AGE_ATTR;
        String attributeName = attribute.getName();
        ComparisonType matchingType = ComparisonType.NOT_EQUAL_TO;

        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, threshold);

        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDate1() throws Exception {
        // Prepare the query
        String dateCompared = "1970-01-14";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.EQUAL_TO;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    
    @Test
    public void testDate2() throws Exception {
        // Prepare the query
        String dateCompared = "1973-01-13";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDate3() throws Exception {
        // Prepare the query
        String dateCompared = "1973-01-13";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.GREATER_THAN_OR_EQUAL_TO;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDate4() throws Exception {
        // Prepare the query
        String dateCompared = "1973-01-13";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDate5() throws Exception {
        // Prepare the query
        String dateCompared = "1973-01-13";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN_OR_EQUAL_TO;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(3));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDate6() throws Exception {
        // Prepare the query
        String dateCompared = "1973-01-13";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.NOT_EQUAL_TO;
        
        // Perform the query
        List<Tuple> returnedResults = getQueryResults(attributeName, matchingType, dateCompared);
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(0));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(1));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(2));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(4));
        expectedResults.add(TestConstants.getSamplePeopleTuples().get(5));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    
    @Test
    public void testDateTime1() throws Exception {
        
        // Prepare the query
        String dateCompared = "1970-01-01T11:11:11";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.EQUAL_TO;
        
        ComparablePredicate comparablePredicate = new ComparablePredicate(attributeName, matchingType, dateCompared);
        ComparableMatcher comparableMatcher = new ComparableMatcher(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        
        // Perform the query
        ScanBasedSourceOperator sourceOperator = new ScanBasedSourceOperator(
                new ScanSourcePredicate(PEOPLE_TABLE_2));
        comparableMatcher.setInputOperator(sourceOperator);
        comparableMatcher.open();
        comparableMatcher.setLimit(Integer.MAX_VALUE);
        comparableMatcher.setOffset(0);
        
        List<Tuple> returnedResults = new ArrayList<>();
        Tuple nextTuple = null;

        while ((nextTuple = comparableMatcher.getNextTuple()) != null) {
            returnedResults.add(nextTuple);
        }
        comparableMatcher.close();        
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstantsRegexSplit.constructSamplePeopleTuples().get(0));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testDateTime2() throws Exception {
        
        // Prepare the query
        String dateCompared = "1970-01-01T11:11:12";
        String attributeName = TestConstants.DATE_OF_BIRTH_ATTR.getName();
        ComparisonType matchingType = ComparisonType.LESS_THAN;
        
        ComparablePredicate comparablePredicate = new ComparablePredicate(attributeName, matchingType, dateCompared);
        ComparableMatcher comparableMatcher = new ComparableMatcher(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        
        // Perform the query
        ScanBasedSourceOperator sourceOperator = new ScanBasedSourceOperator(
                new ScanSourcePredicate(PEOPLE_TABLE_2));
        comparableMatcher.setInputOperator(sourceOperator);
        comparableMatcher.open();
        comparableMatcher.setLimit(Integer.MAX_VALUE);
        comparableMatcher.setOffset(0);
        
        List<Tuple> returnedResults = new ArrayList<>();
        Tuple nextTuple = null;

        while ((nextTuple = comparableMatcher.getNextTuple()) != null) {
            returnedResults.add(nextTuple);
        }
        comparableMatcher.close();        
        
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(TestConstantsRegexSplit.constructSamplePeopleTuples().get(0));

        // check the results
        Assert.assertEquals(expectedResults.size(), returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
}