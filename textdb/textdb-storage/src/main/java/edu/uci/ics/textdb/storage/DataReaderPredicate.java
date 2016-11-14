package edu.uci.ics.textdb.storage;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

/**
 * @author sandeepreddy602
 * @author Zuozhi Wang
 */
public class DataReaderPredicate implements IPredicate {
    private IDataStore dataStore;
    private Query luceneQuery;
    private boolean payloadAdded = false;

    public DataReaderPredicate(Query luceneQuery, IDataStore dataStore) {
        this.dataStore = dataStore;
        this.luceneQuery = luceneQuery;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Query getLuceneQuery() {
        return luceneQuery;
    }
    
    public void setIsPayloadAdded(boolean isPayloadAdded) {
        this.payloadAdded = isPayloadAdded;
    }
    
    public boolean isPayloadAdded() {
        return this.payloadAdded;
    }
    
    public static DataReaderPredicate getScanPredicate(IDataStore dataStore) {
        return new DataReaderPredicate(new MatchAllDocsQuery(), dataStore);
    }
}
