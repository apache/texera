package edu.uci.ics.textdb.storage;

import edu.uci.ics.textdb.api.common.Attribute;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

import java.util.List;

/**
 * Created by sandeepreddy602 on 05-06-2016.
 */
public class DataReaderPredicate implements IPredicate {
    private IDataStore dataStore;
    private Query luceneQuery;
    private String queryString;
    private Analyzer luceneAnalyzer;
    private List<Attribute> attributeList;
    private boolean isSpanInformationAdded = false;

    public DataReaderPredicate(IDataStore dataStore, Query luceneQuery, String queryString, Analyzer analyzer, List<Attribute> attributeList){
        this.dataStore = dataStore;
        this.luceneQuery = luceneQuery;
        this.luceneAnalyzer = analyzer;
        this.queryString = queryString;
        this.attributeList  = attributeList;
    }

    public void setIsSpanInformationAdded(boolean flag){
        isSpanInformationAdded = flag;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Query getLuceneQuery() {
        return luceneQuery;
    }

    public String getQueryString(){return queryString;}

    public Analyzer getLuceneAnalyzer(){return luceneAnalyzer;}

    public List<Attribute> getAttributeList(){return attributeList;}

    public boolean getIsSpanInformationAdded(){return isSpanInformationAdded;}
}
