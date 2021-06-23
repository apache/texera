package edu.uci.ics.texera.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class DataWriterReaderTest {
    
    public static final String PEOPLE_TABLE = "data_writer_reader_test_people";

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

    @Test
    public void testReadWriteData() throws Exception {
        DataReader dataReader = RelationManager.getInstance().getTableDataReader(
                PEOPLE_TABLE, new MatchAllDocsQuery());
        
        Tuple nextTuple = null;
        List<Tuple> returnedTuples = new ArrayList<Tuple>();
        
        dataReader.open();
        while ((nextTuple = dataReader.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
        }
        dataReader.close();
        
        Assert.assertTrue(TestUtils.equals(TestConstants.getSamplePeopleTuples(), returnedTuples));
    }

}
