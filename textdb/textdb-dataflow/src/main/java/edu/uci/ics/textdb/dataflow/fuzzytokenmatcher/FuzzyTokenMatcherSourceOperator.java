package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

public class FuzzyTokenMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private FuzzyTokenPredicate predicate;

    private DataReader dataReader;
    private FuzzyTokenMatcher fuzzyTokenMatcher;
    
    public FuzzyTokenMatcherSourceOperator(FuzzyTokenPredicate predicate, String tableName) 
            throws DataFlowException, StorageException {
        this.predicate = predicate;

        // generate dataReader
        Query luceneQuery = createLuceneQueryObject(this.predicate);   
        this.dataReader = RelationManager.getRelationManager().getTableDataReader(tableName, luceneQuery);
        this.dataReader.setPayloadAdded(true);
        
        // generate FuzzyTokenMatcher
        fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);
        fuzzyTokenMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.fuzzyTokenMatcher;
    }

    @Override
    protected void setUp() throws TextDBException {
        this.outputSchema = this.fuzzyTokenMatcher.getOutputSchema();        
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        return this.fuzzyTokenMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        return this.fuzzyTokenMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TextDBException {        
    }
    
    public static Query createLuceneQueryObject(FuzzyTokenPredicate predicate) throws DataFlowException {
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
                    predicate.getLuceneAnalyzer());
            for (String s : predicate.getQueryTokens()) {
                builder.add(qp.parse(s), Occur.SHOULD);
            }
            return builder.build();
        } catch (ParseException e) {
            throw new DataFlowException(e);
        }
    }

}
