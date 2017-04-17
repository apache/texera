package edu.uci.ics.textdb.exp.regexmatcher;

import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

public class RegexMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private final RegexSourcePredicate predicate;

    private final DataReader dataReader;
    private final RegexMatcher regexMatcher;
    
    public RegexMatcherSourceOperator(RegexSourcePredicate predicate) {
        try {
            this.predicate = predicate;
            
            if (this.predicate.isUseIndex()) {
                this.dataReader = RelationManager.getRelationManager().getTableDataReader(this.predicate.getTableName(), 
                        createLuceneQuery(this.predicate));
            } else {
                this.dataReader = RelationManager.getRelationManager().getTableDataReader(this.predicate.getTableName(), 
                        new MatchAllDocsQuery());
            }
            
            regexMatcher = new RegexMatcher(this.predicate);
            regexMatcher.setInputOperator(dataReader);
            
            this.inputOperator = this.regexMatcher;
        } catch (TextDBException e) {
            throw new RuntimeException(e);
        }

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
    
    public static Query createLuceneQuery(RegexSourcePredicate predicate) throws StorageException {
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
                    predicate.getAttributeNames().stream().toArray(String[]::new), 
                    RelationManager.getRelationManager().getTableAnalyzer(predicate.getTableName()))
                    .parse(queryString);
        } catch (ParseException e) {
            throw new StorageException (e);
        }
        
        return luceneQuery;
    }

}
