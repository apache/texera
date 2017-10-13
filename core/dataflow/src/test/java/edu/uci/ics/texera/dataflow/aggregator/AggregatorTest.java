/**
 * 
 */
package edu.uci.ics.texera.dataflow.aggregator;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.StringField;
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
 *
 */
public class AggregatorTest
{
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

    @AfterClass
    public static void cleanUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    private List<Tuple> getQueryResults(String attributeName, AggregationType aggregatorType, String resAttrName)
    {
        AggregatorPredicate aggPredicate = new AggregatorPredicate(attributeName, aggregatorType, resAttrName);
        Aggregator aggOperator = new Aggregator(aggPredicate);
        
        setPreExecConfigs(aggOperator);
        
        List<Tuple> returnedResults = new ArrayList<>();
        Tuple nextTuple = null;

        while ((nextTuple = aggOperator.getNextTuple()) != null) {
            returnedResults.add(nextTuple);
        }
        return returnedResults;
    }
    
    private void setPreExecConfigs(Aggregator aggOperator)
    {
        ScanBasedSourceOperator sourceOperator = new ScanBasedSourceOperator(
                new ScanSourcePredicate(PEOPLE_TABLE));
        aggOperator.setInputOperator(sourceOperator);
        aggOperator.open();
        aggOperator.setLimit(Integer.MAX_VALUE);
        aggOperator.setOffset(0);
    }

    @Test
    public void testMinAggregation() throws Exception
    {
        Attribute attribute = TestConstants.HEIGHT_ATTR;
        String attributeName = attribute.getName();
        AggregationType aggType = AggregationType.MIN;
        String resultAttributeName = AggregatorTestConstants.RESULT_ATTR_NAME;
        
        IField[] row1 = {new DoubleField(5.50)};
        Schema schema = new Schema(new Attribute(resultAttributeName, AttributeType.DOUBLE));
        List<Tuple> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple(schema, row1));
        
        List<Tuple> returnedResults = getQueryResults(attributeName, aggType, resultAttributeName);
        Assert.assertEquals(1, returnedResults.size());
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
}
