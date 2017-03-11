package edu.uci.ics.textdb.dataflow.regexmatch;

import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

public class RegexMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private RegexPredicate predicate;
    private String tableName;

    private DataReader dataReader;
    private RegexMatcher regexMatcher;
    
    public RegexMatcherSourceOperator(RegexPredicate predicate, String tableName) throws StorageException, DataFlowException {
        this.predicate = predicate;
        this.tableName = tableName;
        
        this.dataReader = RelationManager.getRelationManager().getTableDataReader(this.tableName, 
                createLuceneQuery(this.predicate));
        
        regexMatcher = new RegexMatcher(this.predicate);
        regexMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.regexMatcher;
    }

    @Override
    protected void setUp() throws TextDBException {
        this.outputSchema = regexMatcher.getOutputSchema();        
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        return this.regexMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        return this.regexMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TextDBException {
    }
    
    public static Query createLuceneQuery(RegexPredicate predicate) throws DataFlowException {
        Query luceneQuery;
        String queryString;
        
        // Try to apply translator. If it fails, use scan query.
        try {
            queryString = RegexToGramQueryTranslator.translate(predicate.getRegex()).getLuceneQueryString();
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryString = DataConstants.SCAN_QUERY;
        }

        // Try to parse the query string. It if fails, raise an exception.
        try {
            luceneQuery = new MultiFieldQueryParser(
                    predicate.getAttributeNames().stream().toArray(String[]::new), predicate.getLuceneAnalyzer())
                    .parse(queryString);
        } catch (ParseException e) {
            throw new DataFlowException(e);
        }
        
        return luceneQuery;
    }

}
