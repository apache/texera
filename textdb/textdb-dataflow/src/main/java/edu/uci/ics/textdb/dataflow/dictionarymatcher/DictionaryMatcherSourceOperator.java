
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;

/**
 * @author Sudeep (inkudo)
 * @author Zuozhi Wang (zuozhi)
 * 
 */
public class DictionaryMatcherSourceOperator implements ISourceOperator {

    private ISourceOperator indexSource;
    
    private KeywordMatcherSourceOperator keywordSource;

    private Schema inputSchema;
    private Schema outputSchema;

    private Tuple sourceTuple;
    private String currentDictionaryEntry;

    private final DictionaryPredicate predicate;
    private String tableName;

    private int resultCursor;
    private int limit;
    private int offset;

    /**
     * Constructs a DictionaryMatcher with a dictionary predicate
     * 
     * @param predicate
     * 
     */
    public DictionaryMatcherSourceOperator(DictionaryPredicate predicate, String tableName) {
        this.resultCursor = -1;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.predicate = predicate;
        this.tableName = tableName;
    }

    /**
     * @about Opens dictionary matcher. Must call open() before calling
     *        getNextTuple().
     */
    @Override
    public void open() throws DataFlowException {
        try {
            currentDictionaryEntry = predicate.getNextDictionaryEntry();
            if (currentDictionaryEntry == null) {
                throw new DataFlowException("Dictionary is empty");
            }

            if (predicate.getKeywordMatchingType() == DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED) {
                // For Substring matching, create a scan source operator.
                indexSource = new ScanBasedSourceOperator(tableName);
                indexSource.open();

                // Substring matching's output schema needs to contains span
                // list.
                inputSchema = indexSource.getOutputSchema();
                outputSchema = inputSchema;
                if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                    outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
                }

            } else {
                // For other keyword matching types (conjunction and phrase),
                // create keyword matcher based on index.
                KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                        predicate.getAttributeNames(),
                        predicate.getAnalyzer(),
                        predicate.getKeywordMatchingType());

                keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, tableName);
                keywordSource.open();

                // Other keyword matching types uses a KeywordMatcher, so the
                // output schema is the same as keywordMatcher's schema
                inputSchema = keywordSource.getOutputSchema();
                outputSchema = keywordSource.getOutputSchema();
            }

        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets the next matched tuple. <br>
     *        Returns the tuple with results in spanList. <br>
     * 
     *        Performs SCAN, KEYWORD_BASIC, or KEYWORD_PHRASE depends on the
     *        dictionary predicate. <br>
     * 
     *        DictionaryOperatorType.SCAN: <br>
     *        Scan the tuples using ScanSourceOperator. <br>
     *        For each tuple, loop through the dictionary and find results. <br>
     *        We assume the dictionary is smaller than the data at the source
     *        operator, we treat the data source as the outer relation to reduce
     *        the number of disk IOs. <br>
     * 
     *        DictionaryOperatorType.KEYWORD_BASIC, KEYWORD_PHRASE: <br>
     *        Use KeywordMatcher to find results. <br>
     * 
     *        KEYWORD_BASIC corresponds to KeywordOperatorType.BASIC, which
     *        performs keyword search on the document. The input query is
     *        tokenized. The order of the tokens doesn't matter. <br>
     * 
     *        KEYWORD_PHRASE corresponds to KeywordOperatorType.PHRASE, which
     *        performs phrase search on the document. The input query is
     *        tokenized. The order of the tokens does matter. Stopwords are
     *        treated as placeholders to indicate an arbitary token. <br>
     * 
     */
    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (resultCursor >= limit + offset - 1) {
            return null;
        }
        if (predicate.getKeywordMatchingType() == DataConstants.KeywordMatchingType.PHRASE_INDEXBASED
                || predicate.getKeywordMatchingType() == DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            // For each dictionary entry,
            // get all result from KeywordMatcher.

            while (true) {
                // If there's result from current keywordMatcher, return it.
                if ((sourceTuple = keywordSource.getNextTuple()) != null) {
                    resultCursor++;
                    if (resultCursor >= offset) {
                        return sourceTuple;
                    }
                    continue;
                }
                // If all results from current keywordMatcher are consumed,
                // advance to next dictionary entry, and
                // return null if reach the end of dictionary.
                if ((currentDictionaryEntry = predicate.getNextDictionaryEntry()) == null) {
                    return null;
                }

                // Construct a new KeywordMatcher with the new dictionary entry.
                KeywordMatchingType keywordMatchingType;
                if (predicate.getKeywordMatchingType() == DataConstants.KeywordMatchingType.PHRASE_INDEXBASED) {
                    keywordMatchingType = KeywordMatchingType.PHRASE_INDEXBASED;
                } else {
                    keywordMatchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED;
                }

                keywordSource.close();

                KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                        predicate.getAttributeNames(),
                        predicate.getAnalyzer(), keywordMatchingType);

                keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, tableName);
                keywordSource.open();
            }
        }
        // Substring matching (based on scan)
        else {
            Tuple sourceTuple;
            Tuple resultTuple = null;
            while ((sourceTuple = indexSource.getNextTuple()) != null) {
                if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                    sourceTuple = DataflowUtils.getSpanTuple(sourceTuple.getFields(), new ArrayList<Span>(), outputSchema);
                }
                resultTuple = computeMatchingResult(currentDictionaryEntry, sourceTuple);
                if (resultTuple != null) {
                    resultCursor++;
                }
                if (resultTuple != null && resultCursor >= offset) {
                    break;
                }
            }
            return resultTuple;
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

    /*
     * Advance the cursor of dictionary. if reach the end of the dictionary,
     * advance the cursor of tuples and reset dictionary
     */
    private void advanceDictionaryCursor() throws TextDBException {
        if ((currentDictionaryEntry = predicate.getNextDictionaryEntry()) != null) {
            return;
        }
        predicate.resetDictCursor();
        currentDictionaryEntry = predicate.getNextDictionaryEntry();
    }

    /*
     * Match the key against the Tuple. if there's no match, returns the
     * original Tuple object, if there's a match, return a new Tuple
     * with span list added
     */
    private Tuple computeMatchingResult(String key, Tuple sourceTuple) throws TextDBException {

        List<String> attributeNames = predicate.getAttributeNames();
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : attributeNames) {
            String fieldValue = sourceTuple.getField(attributeName).getValue().toString();
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();

            // if attribute type is not TEXT, then key needs to match the
            // fieldValue exactly
            if (attributeType != AttributeType.TEXT) {
                if (fieldValue.equals(key)) {
                    matchingResults.add(new Span(attributeName, 0, fieldValue.length(), key, fieldValue));
                }
            }
            // if attribute type is TEXT, then key can match a substring of
            // fieldValue
            else {
                String regex = key.toLowerCase();
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();

                    matchingResults.add(new Span(attributeName, start, end, key, fieldValue.substring(start, end)));
                }
            }
        }

        advanceDictionaryCursor();

        if (matchingResults.size() == 0) {
            return null;
        }

        ListField<Span> spanListField = sourceTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return sourceTuple;
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            if (keywordSource != null) {
                keywordSource.close();
            }
            if (indexSource != null) {
                indexSource.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public DictionaryPredicate getPredicate() {
        return this.predicate;
    }

    public String getTableName() {
        return this.tableName;
    }
    
}
