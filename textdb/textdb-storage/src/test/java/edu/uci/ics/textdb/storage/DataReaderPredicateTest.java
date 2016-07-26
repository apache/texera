package edu.uci.ics.textdb.storage;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;

public class DataReaderPredicateTest {
    private DataReaderPredicate dataReaderPredicate;
    private IDataStore dataStore;
    private Query luceneQuery;
    
    @Before
    public void setUp() throws ParseException{
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        QueryParser luceneQueryParser = new QueryParser(
                TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), new  StandardAnalyzer());
        luceneQuery = luceneQueryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(dataStore, luceneQuery,DataConstants.SCAN_QUERY,new StandardAnalyzer(), Arrays.asList(TestConstants.ATTRIBUTES_PEOPLE));
    }
    
    @Test
    public void testGetters(){
        IDataStore dataStoreReturned = dataReaderPredicate.getDataStore();
        Assert.assertSame(dataStore, dataStoreReturned);
        
        Query queryReturned = dataReaderPredicate.getLuceneQuery();
        Assert.assertSame(luceneQuery, queryReturned);
    }
}
