package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/*
 * @author varun bharill, parag saraogi
 * 
 * This class builds the query to perform boolean searches in a lucene index. 
 * The threshold for boolean searches is taken input as a ratio with is converted to integer. 
 * In the worst case if this integer becomes 0, we will set it to 1. 
 */
public class FuzzyTokenPredicate implements IPredicate {

    private String query;
    private Query luceneQuery;
    private ArrayList<String> tokens;
    private List<Attribute> attributeList;
    private String[] fields;
    private Analyzer luceneAnalyzer;
    private double thresholdRatio;
    private int threshold;

    public FuzzyTokenPredicate(String query, List<Attribute> attributeList, Analyzer analyzer,
            double thresholdRatio) throws DataFlowException {
        try {
            this.thresholdRatio = thresholdRatio;
            this.luceneAnalyzer = analyzer;
            this.query = query;
            this.tokens = Utils.tokenizeQuery(analyzer, query);
            this.computeThreshold();
            this.attributeList = attributeList;
            this.extractSearchFields();
            this.luceneQuery = this.createLuceneQueryObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    public List<Attribute> getAttributeList() {
        return this.attributeList;
    }

    public int getThreshold() {
        return this.threshold;
    }

    public ArrayList<String> getQueryTokens() {
        return this.tokens;
    }

    public Analyzer getLuceneAnalyzer() {
        return luceneAnalyzer;
    }

    private void extractSearchFields() {
        this.fields = new String[this.attributeList.size()];
        int i = 0;
        for (Attribute a : this.attributeList) {
            this.fields[i] = a.getFieldName();
            i++;
        }
    }

    /*
     * The input threshold given by the end-user (thresholdRatio data member) is
     * a ratio but boolean search query requires integer as a threshold. In case
     * if the the threshold becomes 0, we will set it by default to 1.
     */
    public void computeThreshold() {
        this.threshold = (int) (this.thresholdRatio * this.tokens.size());
        if (this.threshold == 0) {
            this.threshold = 1;
        }
    }

    private Query createLuceneQueryObject() throws ParseException {
        /*
         * By default the boolean query takes 1024 # of clauses as the max
         * limit. Since our input query has no limitaion on the number of
         * tokens, we have to put a check.
         */
        if (this.threshold > 1024)
            BooleanQuery.setMaxClauseCount(this.threshold + 1);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(this.threshold);
        MultiFieldQueryParser qp = new MultiFieldQueryParser(fields, this.luceneAnalyzer);
        for (String s : this.tokens) {
            builder.add(qp.parse(s), Occur.SHOULD);
        }
        return builder.build();
    }

    public DataReaderPredicate getDataReaderPredicate(IDataStore dataStore) {
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.luceneQuery, dataStore);
        dataReaderPredicate.setIsPayloadAdded(true);
        return dataReaderPredicate;
    }

    public String getQuery() {
        return this.query;
    }

    public double getThresholdRatio() {
        return this.thresholdRatio;
    }

}
