package edu.uci.ics.textdb.dataflow.comparablematcher;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.NumberMatchingType;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.*;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by sweetest.sj on 10/4/16.
 */
public class ComparableMatcherTest {

    private DataWriter dataWriter;
    private DataStore dataStore;
    private Analyzer analyzer;

    private ScanBasedSourceOperator getScanSourceOperator(IDataStore dataStore) throws DataFlowException {
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(dataStore);
        return scanSource;
    }

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, analyzer);
        
        dataWriter.clearData();
        dataWriter.open();
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    public List<ITuple> getDoubleQueryResults(double threshold, Attribute attribute, NumberMatchingType matchingType)
            throws TextDBException {
        // Perform the query
        ComparablePredicate<Double> comparablePredicate = new ComparablePredicate<>(threshold, attribute, matchingType);
        ComparableMatcher<Double> comparableMatcher = new ComparableMatcher<>(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        return getQueryResults(comparableMatcher);
    }

    public List<ITuple> getIntegerQueryResults(int threshold, Attribute attribute, NumberMatchingType matchingType)
            throws TextDBException {
        // Perform the query
        ComparablePredicate<Integer> comparablePredicate = new ComparablePredicate<>(threshold, attribute, matchingType);
        ComparableMatcher<Integer> comparableMatcher = new ComparableMatcher<>(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        return getQueryResults(comparableMatcher);
    }

    public List<ITuple> getDateQueryResults(Date threshold, Attribute attribute, NumberMatchingType matchingType)
            throws TextDBException {
        // Perform the query
        ComparablePredicate<Date> comparablePredicate = new ComparablePredicate<>(threshold, attribute, matchingType);
        ComparableMatcher<Date> comparableMatcher = new ComparableMatcher<>(comparablePredicate);
        setDefaultMatcherConfig(comparableMatcher);
        return getQueryResults(comparableMatcher);
    }

    public void setDefaultMatcherConfig(ComparableMatcher comparableMatcher) throws TextDBException {
        // Perform the query
        ScanBasedSourceOperator sourceOperator = getScanSourceOperator(dataStore);
        comparableMatcher.setInputOperator(sourceOperator);
        comparableMatcher.open();
        comparableMatcher.setLimit(Integer.MAX_VALUE);
        comparableMatcher.setOffset(0);
    }

    public List<ITuple> getQueryResults(ComparableMatcher comparableMatcher) throws TextDBException {
        List<ITuple> returnedResults = new ArrayList<>();
        ITuple nextTuple = null;

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
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        // check the results
        IField[] fields1 = {new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry")};
        IField[] fields2 = {new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        Assert.assertEquals(2, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.LESS_THAN;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));

        // check the results
        Assert.assertEquals(1, returnedResults.size());
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
        DataConstants.NumberMatchingType matchingType = NumberMatchingType.LESS_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry")};
        IField[] fields2 = {new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        // check the results
        Assert.assertEquals(2, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown")};
        IField[] fields2 = {new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry")};
        IField[] fields3 = {new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry")};
        IField[] fields4 = {new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair")};
        IField[] fields5 = {new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields4));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields5));

        // check the results
        Assert.assertEquals(5, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));

        // check the results
        Assert.assertEquals(1, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.NOT_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDoubleQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown") };
        IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields4 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
        IField[] fields5 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields4));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields5));

        // check the results
        Assert.assertEquals(5, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair")};
        IField[] fields2 = {new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        // check the results
        Assert.assertEquals(2, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));

        // check the results
        Assert.assertEquals(1, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry")};
        IField[] fields2 = {new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        // check the results
        Assert.assertEquals(2, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.LESS_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry")};
        IField[] fields2 = {new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair")};
        IField[] fields3 = {new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));

        // check the results
        Assert.assertEquals(3, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.LESS_THAN;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = {new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair")};
        IField[] fields2 = {new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry")};
        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        // check the results
        Assert.assertEquals(2, returnedResults.size());
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
        NumberMatchingType matchingType = NumberMatchingType.NOT_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getIntegerQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown") };
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields4 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
        IField[] fields5 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields4));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields5));

        // check the results
        Assert.assertEquals(5, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type EQUAL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching1() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));

        // check the results
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type GREATER_THAN
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching2() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
        IField[] fields2 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));

        // check the results
        Assert.assertEquals(2, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type GREATER_THAN_OR_NOT_EQUAL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching3() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.GREATER_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);
        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
        IField[] fields3 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));

        // check the results
        Assert.assertEquals(3, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type LESS_THAN
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching4() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.LESS_THAN;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown") };
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));

        // check the results
        Assert.assertEquals(3, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type LESS_THAN_OR_NOT_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching5() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.LESS_THAN_OR_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown") };
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields4 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields4));

        // check the results
        Assert.assertEquals(4, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    /**
     * Verifies the behavior of ComparableMatcher<Date> with matching type NOT_EQAUL_TO
     *
     * @throws Exception
     */
    @Test
    public void testDateMatching6() throws Exception {
        // Prepare the query
        Date threshold = new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973");

        Attribute attribute = TestConstants.DATE_OF_BIRTH_ATTR;
        NumberMatchingType matchingType = NumberMatchingType.NOT_EQUAL_TO;

        // Perform the query
        List<ITuple> returnedResults = getDateQueryResults(threshold, attribute, matchingType);

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                new TextField("Short Brown") };
        IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                new TextField("White Angry") };
        IField[] fields4 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
        IField[] fields5 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

        List<ITuple> expectedResults = new ArrayList<>();
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields1));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields2));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields3));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields4));
        expectedResults.add(new DataTuple(TestConstants.SCHEMA_PEOPLE, fields5));

        // check the results
        Assert.assertEquals(5, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
}
