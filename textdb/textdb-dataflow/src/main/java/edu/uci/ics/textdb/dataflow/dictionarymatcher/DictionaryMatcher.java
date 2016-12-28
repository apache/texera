package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

public class DictionaryMatcher implements IOperator {

    private DictionaryPredicate predicate;

    private IOperator inputOperator;
    private KeywordMatcher keywordMatcher;

    private Schema outputSchema;

    String currentDictionaryEntry;

    private int resultCursor;
    private int limit;
    private int offset;

    private int cursor = CLOSED;

    public DictionaryMatcher(DictionaryPredicate predicate) {
        this.predicate = predicate;

        this.resultCursor = -1;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
    }

    @Override
    public void open() throws DataFlowException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            if (inputOperator == null) {
                throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }

            predicate.resetDictCursor();
            currentDictionaryEntry = predicate.getNextDictionaryEntry();
            if (currentDictionaryEntry == null) {
                throw new DataFlowException("Dictionary is empty");
            }

            KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzer(), predicate.getKeywordMatchingType());

            keywordMatcher = new KeywordMatcher(keywordPredicate);
            keywordMatcher.setInputOperator(inputOperator);

            keywordMatcher.open();
            outputSchema = keywordMatcher.getOutputSchema();

        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = OPENED;
    }

    @Override
    public ITuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (resultCursor >= limit + offset - 1) {
            return null;
        }

        ITuple sourceTuple;
        while (true) {
            // If there's result from current keywordMatcher, return it.
            if ((sourceTuple = keywordMatcher.getNextTuple()) != null) {
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
            keywordMatcher.close();
            inputOperator.close();

            KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzer(), predicate.getKeywordMatchingType());

            keywordMatcher = new KeywordMatcher(keywordPredicate);
            keywordMatcher.setInputOperator(inputOperator);

            keywordMatcher.open();
        }
    }

    @Override
    public void close() throws DataFlowException {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (keywordMatcher != null) {
                keywordMatcher.close();
            }
            if (inputOperator != null) {
                inputOperator.close();
            }
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
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

    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    public IOperator getInputOperator() {
        return inputOperator;
    }

    public DictionaryPredicate getPredicate() {
        return this.predicate;
    }

}
