package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * @author Zuozhi Wang
 * @author prakul
 *
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate {

    private List<String> attributeNames;
    private String query;
    private Query luceneQuery;
    private ArrayList<String> queryTokenList;
    private HashSet<String> queryTokenSet;
    private ArrayList<String> queryTokensWithStopwords;
    private Analyzer luceneAnalyzer;
    private KeywordMatchingType operatorType;

    /*
     * query refers to string of keywords to search for. For Ex. New york if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */
    public KeywordPredicate(String query, List<String> attributeNames, Analyzer luceneAnalyzer,
            KeywordMatchingType operatorType) {
        this.query = query;
        this.queryTokenList = Utils.tokenizeQuery(luceneAnalyzer, query);
        this.queryTokenSet = new HashSet<>(this.queryTokenList);
        this.queryTokensWithStopwords = Utils.tokenizeQueryWithStopwords(query);

        this.attributeNames = attributeNames;
        this.operatorType = operatorType;

        this.luceneAnalyzer = luceneAnalyzer;
    }

    public KeywordMatchingType getOperatorType() {
        return operatorType;
    }

    public String getQuery() {
        return query;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public Query getQueryObject() {
        return this.luceneQuery;
    }

    public ArrayList<String> getQueryTokenList() {
        return this.queryTokenList;
    }

    public HashSet<String> getQueryTokenSet() {
        return this.queryTokenSet;
    }

    public ArrayList<String> getQueryTokensWithStopwords() {
        return this.queryTokensWithStopwords;
    }

    public Analyzer getLuceneAnalyzer() {
        return luceneAnalyzer;
    }

}
