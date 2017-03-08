package edu.uci.ics.textdb.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.utils.Utils;

public class DataWriterReaderTest {
    
    public static final String PEOPLE_TABLE = "data_writer_reader_test_people";

    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/test_tables/" + PEOPLE_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    @Test
    public void testReadWriteData() throws Exception {
        DataReader dataReader = RelationManager.getRelationManager().getTableDataReader(
                PEOPLE_TABLE, new MatchAllDocsQuery());
        
        Tuple nextTuple = null;
        List<Tuple> returnedTuples = new ArrayList<Tuple>();
        
        dataReader.open();
        while ((nextTuple = dataReader.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
        }
        dataReader.close();
        
        boolean equals = containsAllResults(TestConstants.getSamplePeopleTuples(), returnedTuples);
        Assert.assertTrue(equals);
    }

    public static boolean containsAllResults(List<Tuple> expectedResults, List<Tuple> exactResults) {
        expectedResults = Utils.removeFields(expectedResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        exactResults = Utils.removeFields(exactResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);

        if (expectedResults.size() != exactResults.size())
            return false;
        if (!(expectedResults.containsAll(exactResults)) || !(exactResults.containsAll(expectedResults)))
            return false;

        return true;
    }
}
