package edu.uci.ics.textdb.dataflow.common;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.api.exception.DataFlowException;


/**
 * This class is the predicate for Regex.
 * 
 * @author Zuozhi Wang
 * @author Shuying Lai
 *
 */
public class RegexPredicate implements IPredicate {

    private String regex;
    private List<String> attributeNames;

    private Analyzer luceneAnalyzer;


    public RegexPredicate(String regex, List<String> attributeNames, Analyzer analyzer) throws DataFlowException {
        this.regex = regex;
        this.luceneAnalyzer = analyzer;
        this.attributeNames = attributeNames;
    }

    public String getRegex() {
        return regex;
    }

    public Analyzer getLuceneAnalyzer() {
        return this.luceneAnalyzer;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

}
