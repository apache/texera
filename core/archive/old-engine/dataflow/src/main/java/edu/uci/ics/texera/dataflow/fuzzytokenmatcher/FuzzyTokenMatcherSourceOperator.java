package edu.uci.ics.texera.dataflow.fuzzytokenmatcher;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.schema.Schema;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class FuzzyTokenMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private FuzzyTokenSourcePredicate predicate;

    private DataReader dataReader;
    private FuzzyTokenMatcher fuzzyTokenMatcher;
    
    public FuzzyTokenMatcherSourceOperator(FuzzyTokenSourcePredicate predicate) 
            throws DataflowException, StorageException {
        this.predicate = predicate;

        // generate dataReader
        Query luceneQuery = createLuceneQueryObject(this.predicate);   
        this.dataReader = RelationManager.getInstance().getTableDataReader(
                this.predicate.getTableName(), luceneQuery);
        this.dataReader.setPayloadAdded(true);
        
        // generate FuzzyTokenMatcher
        fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);
        fuzzyTokenMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.fuzzyTokenMatcher;
    }

    @Override
    protected void setUp() throws TexeraException {
        this.outputSchema = this.fuzzyTokenMatcher.getOutputSchema();        
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        return this.fuzzyTokenMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        return this.fuzzyTokenMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TexeraException {        
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

    public static Query createLuceneQueryObject(FuzzyTokenPredicate predicate) throws DataflowException {
        try {
            /*
             * By default the boolean query takes 1024 # of clauses as the max
             * limit. Since our input query has no limitaion on the number of
             * tokens, we have to put a check.
             */
            if (predicate.getThreshold() > 1024)
                BooleanQuery.setMaxClauseCount(predicate.getThreshold()  + 1);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(predicate.getThreshold());
            MultiFieldQueryParser qp = new MultiFieldQueryParser(
                    predicate.getAttributeNames().stream().toArray(String[]::new),
                    LuceneAnalyzerConstants.getLuceneAnalyzer(predicate.getLuceneAnalyzerStr()));
            for (String s : predicate.getQueryTokens()) {
                builder.add(qp.parse(s), Occur.SHOULD);
            }
            return builder.build();
        } catch (ParseException e) {
            throw new DataflowException(e);
        }
    }

}
