/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {
    
    private IDataWriter dataWriter;
    private ScanBasedSourceOperator scanBasedSourceOperator;
    private IDataReader dataReader;
    private IDataStore dataStore;
    private Analyzer lucneAnalyzer;
    private Query query;
    private IPredicate dataReaderPredicate;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        lucneAnalyzer = new  StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, lucneAnalyzer);
        QueryParser queryParser = new QueryParser(
                TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), lucneAnalyzer);
        query = queryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(dataStore, query,
                DataConstants.SCAN_QUERY, lucneAnalyzer, Arrays.asList(TestConstants.ATTRIBUTES_PEOPLE[0]));
        dataReader = new DataReader(dataReaderPredicate);
        
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
        scanBasedSourceOperator = new ScanBasedSourceOperator(dataReader);
    }
    
    @After
    public void cleanUp() throws Exception{
        dataWriter.clearData();
    }
    
    @Test
    public void testFlow() throws DataFlowException, ParseException{
        List<ITuple> actualTuples = TestConstants.getSamplePeopleTuples();
        scanBasedSourceOperator.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        List<ITuple> returnedTuples = new ArrayList<ITuple>();
        while((nextTuple  = scanBasedSourceOperator.getNextTuple()) != null){
        	returnedTuples.add(nextTuple);
            numTuples ++;
        }
        Assert.assertEquals(actualTuples.size(), numTuples);
        boolean contains = TestUtils.containsAllResults(actualTuples, returnedTuples);
		Assert.assertTrue(contains);
        scanBasedSourceOperator.close();
    }
    
}
