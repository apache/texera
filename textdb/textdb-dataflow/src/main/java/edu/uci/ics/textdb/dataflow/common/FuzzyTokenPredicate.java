package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * @author varun bharill, parag saraogi
 * 
 */
public class FuzzyTokenPredicate implements IPredicate {
	
    private IDataStore dataStore;
    private String query;
    private Query luceneQuery;
    private String[] tokens;
    private List<Attribute> attributeList;
    private String[] fields;
    private Analyzer analyzer;
    private double thresholdRatio;
    private int threshold;
    private int docCountInSource;
    private boolean getSpan;
    
    public FuzzyTokenPredicate(String query, List<Attribute> attributeList, Analyzer analyzer,IDataStore dataStore, double thresholdRatio, boolean getSpan) throws DataFlowException{
        try {
        	this.thresholdRatio = thresholdRatio;
        	this.dataStore = dataStore;
        	this.analyzer = analyzer;
        	this.getSpan= getSpan;
            this.docCountInSource = dataStore.getNumDocuments();
            this.query = query;
            this.tokens = this.getSplit(this.query);
            setThreshold();
            this.attributeList = attributeList;
            setSearchFields();
            this.luceneQuery = this.createLuceneQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
    private String[] getSplit(String inp) {
    	return null;
    }
    
    private void setSearchFields() {
    	return;
	}

	public void setThreshold() {
    	return;
    }
	
    private Query createLuceneQueryObject() throws ParseException {
    	return null;
    }

    public DataReaderPredicate getDataReaderPredicate() {
    	return null;
    }


}
