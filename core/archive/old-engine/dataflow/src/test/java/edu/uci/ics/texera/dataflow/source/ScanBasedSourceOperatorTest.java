/**
 * 
 */
package edu.uci.ics.texera.dataflow.source;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {

    public static final String PEOPLE_TABLE = "scan_source_test_people";
    
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
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    @Test
    public void testFlow() throws TexeraException, ParseException {
        List<Tuple> actualTuples = TestConstants.getSamplePeopleTuples();
        
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(new ScanSourcePredicate(PEOPLE_TABLE));
        scanBasedSourceOperator.open();
        Tuple nextTuple = null;
        int numTuples = 0;
        List<Tuple> returnedTuples = new ArrayList<Tuple>();
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
