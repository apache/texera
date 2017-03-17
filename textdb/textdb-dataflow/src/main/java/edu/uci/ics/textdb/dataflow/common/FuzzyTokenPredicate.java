package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;

/*
 * @author varun bharill, parag saraogi
 * 
 * This class builds the query to perform boolean searches in a lucene index. 
 * The threshold for boolean searches is taken input as a ratio with is converted to integer. 
 * In the worst case if this integer becomes 0, we will set it to 1. 
 */
public class FuzzyTokenPredicate implements IPredicate {

    private String query;
    private ArrayList<String> tokens;
    private List<String> attributeNames;
    private Analyzer luceneAnalyzer;
    private double thresholdRatio;
    private int threshold;

    public FuzzyTokenPredicate(String query, List<String> attributeNames, Analyzer analyzer,
            double thresholdRatio) throws DataFlowException {
        this.thresholdRatio = thresholdRatio;
        this.luceneAnalyzer = analyzer;
        this.query = query;
        this.tokens = DataflowUtils.tokenizeQuery(analyzer, query);
        this.computeThreshold();
        this.attributeNames = attributeNames;
    }

    public List<String> getAttributeNames() {
        return this.attributeNames;
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

    public String getQuery() {
        return this.query;
    }

    public double getThresholdRatio() {
        return this.thresholdRatio;
    }

}
