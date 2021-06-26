/**
 *
 */
package edu.uci.ics.texera.dataflow.aggregator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import junit.framework.Assert;

/**
 * @author avinash
 * <p>
 * This test uses the People_Table defined in TestConstants to verify the aggregator operator results
 */
public class AggregatorTest {
    public static final String PEOPLE_TABLE = "aggregation_test_people";

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
    }

    private void setPreExecConfigs(Aggregator aggOperator) {
        ScanBasedSourceOperator sourceOperator = new ScanBasedSourceOperator(
                new ScanSourcePredicate(PEOPLE_TABLE));
        aggOperator.setInputOperator(sourceOperator);
        aggOperator.open();
        aggOperator.setLimit(Integer.MAX_VALUE);
        aggOperator.setOffset(0);
    }

    //TEST 1: Find min in height column 
    @Test
    public void testMinHeightAggregation() throws Exception {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.MIN;
        String resultAttributeName = AggregatorTestConstants.MIN_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName, aggType, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new DoubleField(5.50)};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 2: Find max in height column
    @Test
    public void testMaxHeightAggregation() throws Exception {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.MAX;
        String resultAttributeName = AggregatorTestConstants.MAX_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName, aggType, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new DoubleField(6.10)};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 3: Find average in height column
    @Test
    public void testAvgHeightAggregation() throws Exception {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.AVERAGE;
        String resultAttributeName = AggregatorTestConstants.AVG_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName, aggType, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new DoubleField((5.50 + 5.95 + 6.10 + 6.06 + 5.99 + 5.99) / 6.0)};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 4: Find count in height column
    @Test
    public void testCountHeightAggregation() throws Exception {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.COUNT;
        String resultAttributeName = AggregatorTestConstants.COUNT_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName, aggType, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new IntegerField(6)};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.INTEGER));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 5: Find sum in height column
    @Test
    public void testSumHeightAggregation() throws Exception {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.SUM;
        String resultAttributeName = AggregatorTestConstants.SUM_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName, aggType, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new DoubleField((5.50 + 5.95 + 6.10 + 6.06 + 5.99 + 5.99))};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 6: Find min and max in height column
    @Test
    public void testMinHeightMaxHeightAggregation() throws Exception {
        Attribute attribute1 = TestConstants.HEIGHT_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MIN;

        Attribute attribute2 = TestConstants.HEIGHT_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MAX;

        String resultAttributeName1 = AggregatorTestConstants.MIN_HEIGHT_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MAX_HEIGHT_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);

        IField[] row1 = {new DoubleField(5.50), new DoubleField(6.10)};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DOUBLE), new Attribute(resultAttributeName2, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 7: Find min in height and max in age column
    @Test
    public void testMinHeightMaxAgeAggregation() throws Exception {
        Attribute attribute1 = TestConstants.HEIGHT_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MIN;

        Attribute attribute2 = TestConstants.AGE_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MAX;

        String resultAttributeName1 = AggregatorTestConstants.MIN_HEIGHT_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MAX_AGE_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);

        IField[] row1 = {new DoubleField(5.50), new IntegerField(46)};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DOUBLE), new Attribute(resultAttributeName2, AttributeType.INTEGER));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 8: Find min in DOB and max in age column
    @Test
    public void testMinDOBMaxAgeAggregation() throws Exception {
        Attribute attribute1 = TestConstants.DATE_OF_BIRTH_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MIN;

        Attribute attribute2 = TestConstants.AGE_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MAX;

        String resultAttributeName1 = AggregatorTestConstants.MIN_DATE_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MAX_AGE_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);

        IField[] row1 = {new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")), new IntegerField(46)};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DATE), new Attribute(resultAttributeName2, AttributeType.INTEGER));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 9: Find max in DOB and min in Name (String) column
    @Test
    public void testMaxDOBMinNameAggregation() throws Exception {
        Attribute attribute1 = TestConstants.DATE_OF_BIRTH_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MAX;

        Attribute attribute2 = TestConstants.FIRST_NAME_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MIN;

        String resultAttributeName1 = AggregatorTestConstants.MAX_DATE_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MIN_FIRST_NAME_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);

        IField[] row1 = {new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new StringField("Mary brown")};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DATE), new Attribute(resultAttributeName2, AttributeType.STRING));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 10: Find max in DOB, min in Name (String) and max in Name (String) column
    @Test
    public void testMaxDOBMinNameMaxNameAggregation() throws Exception {
        Attribute attribute1 = TestConstants.DATE_OF_BIRTH_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MAX;

        Attribute attribute2 = TestConstants.FIRST_NAME_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MIN;

        Attribute attribute3 = TestConstants.FIRST_NAME_ATTR;
        String attributeName3 = attribute2.getName();
        AggregationType aggType3 = AggregationType.MAX;

        String resultAttributeName1 = AggregatorTestConstants.MAX_DATE_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MIN_FIRST_NAME_RESULT_ATTR_NAME;
        String resultAttributeName3 = AggregatorTestConstants.MAX_FIRST_NAME_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);
        AggregationAttributeAndResult aggEntity3 = new AggregationAttributeAndResult(attributeName3, aggType3, resultAttributeName3);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);
        aggEntitiesList.add(aggEntity3);

        IField[] row1 = {new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new StringField("Mary brown"), new StringField("tom hanks")};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DATE), new Attribute(resultAttributeName2, AttributeType.STRING), new Attribute(resultAttributeName3, AttributeType.STRING));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 11: Find max in DOB and min in Description (String) column
    @Test
    public void testMaxDOBMinDescriptionAggregation() throws Exception {
        Attribute attribute1 = TestConstants.DATE_OF_BIRTH_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MAX;

        Attribute attribute2 = TestConstants.DESCRIPTION_ATTR;
        String attributeName2 = attribute2.getName();
        AggregationType aggType2 = AggregationType.MIN;

        String resultAttributeName1 = AggregatorTestConstants.MAX_DATE_RESULT_ATTR_NAME;
        String resultAttributeName2 = AggregatorTestConstants.MIN_DESCRIPTION_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity1 = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName1);
        AggregationAttributeAndResult aggEntity2 = new AggregationAttributeAndResult(attributeName2, aggType2, resultAttributeName2);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity1);
        aggEntitiesList.add(aggEntity2);

        IField[] row1 = {new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Lin Clooney is Short and lin clooney is Angry")};
        Schema schema = new Schema(new Attribute(resultAttributeName1, AttributeType.DATE), new Attribute(resultAttributeName2, AttributeType.TEXT));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    //TEST 12: Find max in DOB column 
    @Test
    public void testMaxDateAggregation() throws Exception {
        Attribute attribute1 = TestConstants.DATE_OF_BIRTH_ATTR;
        String attributeName1 = attribute1.getName();
        AggregationType aggType1 = AggregationType.MAX;
        String resultAttributeName = AggregatorTestConstants.MAX_DATE_RESULT_ATTR_NAME;

        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult(attributeName1, aggType1, resultAttributeName);

        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);

        IField[] row1 = {new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974"))};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DATE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));

        List<Tuple> returnedResults = getQueryResults(aggEntitiesList);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

    @AfterClass
    public static void cleanUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    private List<Tuple> getQueryResults(List<AggregationAttributeAndResult> aggregationItems) {
        AggregatorPredicate aggPredicate = new AggregatorPredicate(aggregationItems);
        Aggregator aggOperator = new Aggregator(aggPredicate);

        setPreExecConfigs(aggOperator);

        List<Tuple> returnedResults = new ArrayList<>();
        Tuple nextTuple = null;

        while ((nextTuple = aggOperator.getNextTuple()) != null) {
            returnedResults.add(nextTuple);
        }
        return returnedResults;
    }

}
