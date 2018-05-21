package edu.uci.ics.texera.dataflow.regexmatcher;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.schema.Schema;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;

public class RegexMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private final RegexSourcePredicate predicate;

    private final DataReader dataReader;
    private final RegexMatcher regexMatcher;
    
    public RegexMatcherSourceOperator(RegexSourcePredicate predicate) throws StorageException, DataflowException {
        this.predicate = predicate;
        
        if (this.predicate.isUseIndex()) {
            this.dataReader = RelationManager.getInstance().getTableDataReader(this.predicate.getTableName(), 
                    createLuceneQuery(this.predicate));
        } else {
            this.dataReader = RelationManager.getInstance().getTableDataReader(this.predicate.getTableName(), 
                    new MatchAllDocsQuery());
        }
        
        regexMatcher = new RegexMatcher(this.predicate);
        regexMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.regexMatcher;
    }

    @Override
    protected void setUp() throws TexeraException {
        this.outputSchema = regexMatcher.getOutputSchema();        
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        return this.regexMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        return this.regexMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TexeraException {
    }
    
    public static Query createLuceneQuery(RegexSourcePredicate predicate) throws StorageException {
        Query luceneQuery;
        String queryString;
        
        // Try to apply translator. If it fails, use scan query.
        try {
            queryString = RegexToGramQueryTranslator.translate(predicate.getRegex()).getLuceneQueryString();
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryString = DataflowUtils.LUCENE_SCAN_QUERY;
        }

        // Try to parse the query string. It if fails, raise an exception.
        try {
            luceneQuery = new MultiFieldQueryParser(
                    predicate.getAttributeNames().stream().toArray(String[]::new), 
                    RelationManager.getInstance().getTableAnalyzer(predicate.getTableName()))
                    .parse(queryString);
        } catch (ParseException e) {
            throw new StorageException (e);
        }
        
        return luceneQuery;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema == null || inputSchema.length == 0) {
            if (outputSchema == null) {
                open();
                close();
            }
            return getOutputSchema();
        }
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }

}
