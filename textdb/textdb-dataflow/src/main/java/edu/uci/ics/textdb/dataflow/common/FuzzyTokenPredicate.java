package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import java.util.ArrayList;
import java.util.List;

/*
 * @author varun bharill, parag saraogi
 * 
 * This class builds the query to perform boolean searches in a lucene index. 
 */
public class FuzzyTokenPredicate implements IPredicate {
	
    private IDataStore dataStore;
    private String query;
    private Query luceneQuery;
    private ArrayList<String> tokens;
    private List<Attribute> attributeList;
    private String[] fields;
    private Analyzer luceneAnalyzer;
    private double thresholdRatio;
    private int threshold;
    private boolean isSpanInformationAdded;
    
    public FuzzyTokenPredicate(String query, List<Attribute> attributeList, Analyzer analyzer,IDataStore dataStore, double thresholdRatio, boolean isSpanInformationAdded) throws DataFlowException{
        try {
        	this.thresholdRatio = thresholdRatio;
        	this.dataStore = dataStore;
        	this.luceneAnalyzer = analyzer;
        	this.isSpanInformationAdded= isSpanInformationAdded;
            this.query = query;
            this.tokens = Utils.tokenizeQuery(analyzer, query);
            this.computeThreshold();
            this.attributeList = attributeList;
            this.fields = this.extractSearchFields();
            this.luceneQuery = this.createLuceneQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
    private String[] extractSearchFields() {
    	return null;
	}

    /*
     * The input threshold given by the end-user (thresholdRatio data member) is a ratio
     * but boolean search query requires integer as a threshold.
     */
	public void computeThreshold() {
    	return;
    }
	
    private Query createLuceneQueryObject() throws ParseException {
    	return null;
    }

    public DataReaderPredicate getDataReaderPredicate() {
    	return null;
    }


}
