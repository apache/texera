package edu.uci.ics.textdb.storage;

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
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

public class LuceneDataWriterReaderTest {
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private IDataStore dataStore;
    private Analyzer analyzer;
    private Query query;
    
    @Before
    public void setUp() throws ParseException{
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new  StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer );
        QueryParser queryParser = new QueryParser(
                TestConstants.ATTRIBUTES_PEOPLE.get(0).getFieldName(), analyzer);
        query = queryParser.parse(LuceneConstants.SCAN_QUERY);
        dataReader = new LuceneDataReader(dataStore, query);
    }
    
    @Test
    public void testReadWriteData() throws Exception{
        dataWriter.clearData();
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        dataWriter.writeData(tuples);
        Assert.assertEquals(tuples.size(), dataStore.getNumDocuments());
        dataReader.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple  = dataReader.getNextTuple()) != null){
            //Checking if the tuple retrieved is present in the samplesTuples
            boolean contains = contains(tuples, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(tuples.size(), numTuples);
        dataReader.close();
    }

    private boolean contains(List<ITuple> sampleTuples, ITuple actualTuple) {
        boolean contains = false;
        int schemaSize = TestConstants.ATTRIBUTES_PEOPLE.size();
        for (ITuple sampleTuple : sampleTuples) {
            contains = true;
            for (int i = 0; i < schemaSize; i++) {
                if(!sampleTuple.getField(i).equals(actualTuple.getField(i))){
                    contains = false;
                }
            }
            if(contains){
                return contains;
            }
        }
        return contains;
    }
}
