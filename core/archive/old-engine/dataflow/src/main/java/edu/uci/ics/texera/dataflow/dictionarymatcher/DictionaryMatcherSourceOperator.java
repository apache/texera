
package edu.uci.ics.texera.dataflow.dictionarymatcher;


import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;


/**
 * @author Sudeep (inkudo)
 * @author Zuozhi Wang (zuozhi)
 * @author Chang Liu
 * 
 */
public class DictionaryMatcherSourceOperator implements ISourceOperator {

    private ISourceOperator indexSource;
    
    private KeywordMatcherSourceOperator keywordSource;
    private DictionaryMatcher dictionaryMatcher;

    private Schema outputSchema;

    private String currentDictionaryEntry;

    private final DictionarySourcePredicate predicate;

    private int limit;
    private int offset;

    private HashMap<String, Tuple> tupleIDMap; // map of tuple's ID to the tuple itself
    private Map<String, List<Span>> tupleResultMap; // map of tuple's ID to the tuples's results (for Conjunction and Phrase)
    private boolean resultMapPopulated = false;
    
    private Iterator<String> resultIterator;

    private int cursor = CLOSED;  //Flag for computing matching results for CONJUNCTION and PHRASE.

    /**
     * Constructs a DictionaryMatcher with a dictionary predicate.
     *
     * Performs SUBSTRING_SCAN, PHRASE_INDEX, or CONJUNCTION_INDEX
     * depending on the dictionary predicate.
     *
     * DictionaryOperatorType.SUBSTRING_SCAN: <br>
     * Scan the tuples using ScanSourceOperator followed by a Dictionary Matcher. <br>
     * For each tuple, loop through the dictionary entries and generate results from
     * DictionaryMatcher. <br>
     *
     * DictionaryOperatorType.PHRASE_INDEX, CONJUNCTION_INDEX: <br>
     * Loop through the dictionary entries.
     * For each entry, use an index-based KeywordMatcher to get the matching results.
     * Maintain a HashMap </Tuple_ID, Tuple> to add all the matching results
     * into the spanlist of each input tuple.
     *
     * CONJUNCTION_INDEX corresponds to KeywordOperatorType.BASIC, which
     * performs keyword search on the document. The input query is
     * tokenized. The order of the tokens doesn't matter. <br>
     *
     * PHRASE_INDEX corresponds to KeywordOperatorType.PHRASE, which
     * performs phrase search on the document. The input query is
     * tokenized. The order of the tokens does matter. Stopwords are
     * treated as placeholders to indicate an arbitary token. <br>
     * 
     * @param predicate
     * 
     */
    public DictionaryMatcherSourceOperator(DictionarySourcePredicate predicate) {
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.predicate = predicate;
        
        this.tupleIDMap = new HashMap<>();
        this.tupleResultMap = new HashMap<>();
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }

        currentDictionaryEntry = predicate.getDictionary().getNextEntry();

        if (predicate.getKeywordMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED
                || predicate.getKeywordMatchingType() == KeywordMatchingType.REGEX) {

            // For Substring matching and Regex matching, create a scan source operator followed by a dictionary matcher.
            indexSource = new ScanBasedSourceOperator(new ScanSourcePredicate(predicate.getTableName()));

            dictionaryMatcher = new DictionaryMatcher(new DictionaryPredicate(predicate.getDictionary(), predicate.getAttributeNames(),
                    predicate.getAnalyzerString(), predicate.getKeywordMatchingType(), predicate.getSpanListName()));

            dictionaryMatcher.setInputOperator(indexSource);
            dictionaryMatcher.open();
            outputSchema = dictionaryMatcher.getOutputSchema();

        } else {
            // For other keyword matching types (CONJUNCTION and PHRASE),
            // create an index-based keyword source operator.
            keywordSource = new KeywordMatcherSourceOperator(new KeywordSourcePredicate(
                    currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzerString(),
                    predicate.getKeywordMatchingType(),
                    predicate.getTableName(),
                    predicate.getSpanListName()));

            keywordSource.open();

            // Other keyword matching types uses a KeywordMatcher, so the
            // output schema is the same as keywordMatcher's schema.
            outputSchema = keywordSource.getOutputSchema();
        }
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor >= limit + offset) {
            return null;
        }
        
        if (predicate.getKeywordMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED
                || predicate.getKeywordMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            // For each dictionary entry, get all results from KeywordMatcher.
            if(! resultMapPopulated){
                computeMatchingResults();
                resultIterator = tupleIDMap.keySet().iterator();
                
                resultMapPopulated = true;
            }
            while (true) {
                if (resultIterator.hasNext()) {
                    cursor++;
                    String tupleID = resultIterator.next();
                    Tuple resultTuple = new Tuple.Builder(tupleIDMap.get(tupleID))
                        .add(predicate.getSpanListName(), AttributeType.LIST, 
                                new ListField<Span>(tupleResultMap.get(tupleID))).build();
                    if (cursor > offset) {
                        return resultTuple;
                    }
                    continue;
                } else {
                    return null;
                }
            }
        }

        // Substring matching or regex matching (scan based)
        else {
            while(true) {
                Tuple inputTuple;
                if ((inputTuple = dictionaryMatcher.getNextTuple()) != null) {
                    cursor++;
                    if(cursor > offset) {
                        return inputTuple;
                    }
                    continue;
                } else {
                    return null;
                }
            }
        }
    }


    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return this.limit;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return this.offset;
    }

    /***
     *  Maintain a HashMap </Tuple_ID, Tuple> to compute all the keyword
     *  matching results for each tuple.
     *
     */

    @SuppressWarnings("unchecked")
	private void computeMatchingResults(){
        Tuple inputTuple;
        
        while(true) {
            while((inputTuple = keywordSource.getNextTuple()) != null){
                String tupleID = inputTuple.getField(SchemaConstants._ID).getValue().toString();
                ListField<Span> keywordResultsField = inputTuple.getField(predicate.getSpanListName(), ListField.class);
                List<Span> keywordResults = keywordResultsField.getValue();
                
                if (tupleResultMap.containsKey(tupleID)) {
                    tupleResultMap.get(tupleID).addAll(keywordResults);
                } else {
                    tupleIDMap.put(tupleID, new Tuple.Builder(inputTuple).remove(predicate.getSpanListName()).build());
                    tupleResultMap.put(tupleID, new ArrayList<>(keywordResults));
                }
            }

            if ((currentDictionaryEntry = predicate.getDictionary().getNextEntry()) == null) {
                 return;
            }

            keywordSource.close();

            KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzerString(), predicate.getKeywordMatchingType(),
                    predicate.getTableName(),
                    predicate.getSpanListName());

            keywordSource = new KeywordMatcherSourceOperator(keywordSourcePredicate);
            keywordSource.open();
        }
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataflowException {
        try {
            if (keywordSource != null) {
                keywordSource.close();
            }
            if (indexSource != null) {
                indexSource.close();
            }

            if (dictionaryMatcher != null){
                dictionaryMatcher.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataflowException(e.getMessage(), e);
        }
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

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public DictionaryPredicate getPredicate() {
        return this.predicate;
    }
    
}
