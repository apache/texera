package edu.uci.ics.textdb.dataflow.keywordmatch;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * KeywordMatcherSourceOperator is a source operator with a keyword query.
 * 
 * @author Zuozhi Wang
 * @author Zhenfeng Qi
 *
 */
public class KeywordMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {

    private KeywordPredicate predicate;
    private String tableName;

    private String keywordQuery;

    private DataReader dataReader;
    private KeywordMatcher keywordMatcher;
    
    private Schema inputSchema;

    public KeywordMatcherSourceOperator(KeywordPredicate predicate, String tableName) 
            throws DataFlowException, StorageException {
        this.predicate = predicate;
        this.tableName = tableName;
        
        this.keywordQuery = predicate.getQuery();
        
        // input schema must be specified before creating query
        this.inputSchema = RelationManager.getRelationManager().getTableDataStore(tableName).getSchema();
        
        // generate dataReader
        Query luceneQuery = createLuceneQueryObject();

        this.dataReader = RelationManager.getRelationManager().getTableDataReader(tableName, luceneQuery);
        this.dataReader.setPayloadAdded(true);
        
        // generate KeywordMatcher
        keywordMatcher = new KeywordMatcher(predicate);
        keywordMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.keywordMatcher;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    @Override
    protected void setUp() throws DataFlowException {
        this.outputSchema = keywordMatcher.getOutputSchema();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        return this.keywordMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        return this.keywordMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws DataFlowException {
    }

    /**
     * Source Operator doesn't need an input operator. Calling setInputOperator
     * won't have any effects.
     */
    @Override
    public void setInputOperator(IOperator inputOperator) {
    }

    public KeywordPredicate getPredicate() {
        return this.predicate;
    }
    
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Creates a Query object as a boolean Query on all attributes Example: For
     * creating a query like (TestConstants.DESCRIPTION + ":lin" + " AND " +
     * TestConstants.LAST_NAME + ":lin") we provide a list of AttributeFields
     * (Description, Last_name) to search on and a query string (lin)
     *
     * @return Query
     * @throws ParseException
     * @throws DataFlowException
     */
    private Query createLuceneQueryObject() throws DataFlowException {
        Query query = null;
        if (this.predicate.getOperatorType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            query = buildConjunctionQuery();
        }
        if (this.predicate.getOperatorType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            query = buildPhraseQuery();
        }
        if (this.predicate.getOperatorType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
            query = buildScanQuery();
        }

        return query;
    }

    private Query buildConjunctionQuery() throws DataFlowException {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException(
                        "KeywordPredicate: Fields other than STRING and TEXT are not supported yet");
            }

            if (attributeType == AttributeType.STRING) {
                Query termQuery = new TermQuery(new Term(attributeName, this.keywordQuery));
                booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            if (attributeType == AttributeType.TEXT) {
                BooleanQuery.Builder fieldQueryBuilder = new BooleanQuery.Builder();
                for (String token : this.predicate.getQueryTokenSet()) {
                    Query termQuery = new TermQuery(new Term(attributeName, token.toLowerCase()));
                    fieldQueryBuilder.add(termQuery, BooleanClause.Occur.MUST);
                }
                booleanQueryBuilder.add(fieldQueryBuilder.build(), BooleanClause.Occur.SHOULD);
            }

        }

        return booleanQueryBuilder.build();
    }

    private Query buildPhraseQuery() throws DataFlowException {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException(
                        "KeywordPredicate: Fields other than STRING and TEXT are not supported yet");
            }

            if (attributeType == AttributeType.STRING) {
                Query termQuery = new TermQuery(new Term(attributeName, this.keywordQuery));
                booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            if (attributeType == AttributeType.TEXT) {
                if (this.predicate.getQueryTokenList().size() == 1) {
                    Query termQuery = new TermQuery(new Term(attributeName, this.keywordQuery.toLowerCase()));
                    booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
                } else {
                    PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
                    for (int i = 0; i < this.predicate.getQueryTokensWithStopwords().size(); i++) {
                        if (!StandardAnalyzer.STOP_WORDS_SET
                                .contains(this.predicate.getQueryTokensWithStopwords().get(i))) {
                            phraseQueryBuilder.add(new Term(attributeName,
                                    this.predicate.getQueryTokensWithStopwords().get(i).toLowerCase()), i);
                        }
                    }
                    PhraseQuery phraseQuery = phraseQueryBuilder.build();
                    booleanQueryBuilder.add(phraseQuery, BooleanClause.Occur.SHOULD);
                }
            }

        }

        return booleanQueryBuilder.build();
    }

    private Query buildScanQuery() throws DataFlowException {
        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException(
                        "KeywordPredicate: Fields other than STRING and TEXT are not supported yet");
            }
        }

        return new MatchAllDocsQuery();
    }


}
