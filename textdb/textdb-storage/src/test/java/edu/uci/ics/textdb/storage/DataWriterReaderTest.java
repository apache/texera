package edu.uci.ics.textdb.storage;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class DataWriterReaderTest {
    private DataWriter dataWriter;
    private IDataReader dataReader;
    private IDataStore dataStore;
    private DataReaderPredicate dataReaderPredicate;
    private Analyzer luceneAnalyzer;
    private Query query;

    @Before
    public void setUp() throws ParseException {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        luceneAnalyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        QueryParser queryParser = new QueryParser(TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), luceneAnalyzer);
        query = queryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(query, dataStore);
        dataReader = new DataReader(dataReaderPredicate);
    }

    @Test
    public void testReadWriteData() throws Exception {
        List<ITuple> actualTuples = TestConstants.getSamplePeopleTuples();
        
        dataWriter.clearData();
        dataWriter.open();
        for (ITuple tuple : actualTuples) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
        
        Assert.assertEquals(dataStore.getNumDocuments(), actualTuples.size());
        
        dataReader.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        List<ITuple> returnedTuples = new ArrayList<ITuple>();
        while ((nextTuple = dataReader.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
            numTuples++;
        }
        Assert.assertEquals(actualTuples.size(), numTuples);
        boolean contains = containsAllResults(actualTuples, returnedTuples);
        Assert.assertTrue(contains);
        dataReader.close();
    }

    public static boolean containsAllResults(List<ITuple> expectedResults, List<ITuple> exactResults) {
        expectedResults = Utils.removeFields(expectedResults, SchemaConstants.PAYLOAD);
        exactResults = Utils.removeFields(exactResults, SchemaConstants.PAYLOAD);

        if (expectedResults.size() != exactResults.size())
            return false;
        if (!(expectedResults.containsAll(exactResults)) || !(exactResults.containsAll(expectedResults)))
            return false;

        return true;
    }
}
