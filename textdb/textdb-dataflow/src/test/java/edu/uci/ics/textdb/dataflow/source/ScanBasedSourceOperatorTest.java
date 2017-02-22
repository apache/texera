/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {

    public static final String PEOPLE_TABLE = "scan_source_test_people";
    
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/test_tables/" + PEOPLE_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    @Test
    public void testFlow() throws TextDBException, ParseException {
        List<ITuple> actualTuples = TestConstants.getSamplePeopleTuples();
        
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(PEOPLE_TABLE);
        scanBasedSourceOperator.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        List<ITuple> returnedTuples = new ArrayList<ITuple>();
        while ((nextTuple = scanBasedSourceOperator.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
            numTuples++;
        }
        Assert.assertEquals(actualTuples.size(), numTuples);
        boolean contains = TestUtils.equals(actualTuples, returnedTuples);
        Assert.assertTrue(contains);
        scanBasedSourceOperator.close();
    }

}
