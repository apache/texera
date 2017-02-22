package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

public class DictionaryMatcher implements IOperator {

    private DictionaryPredicate predicate;

    private IOperator inputOperator;
    private DictionaryTupleCacheOperator cacheOperator;
    private KeywordPredicate keywordPredicate;
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

            keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzer(), predicate.getKeywordMatchingType());

            keywordMatcher = new KeywordMatcher(keywordPredicate);
            
            cacheOperator = new DictionaryTupleCacheOperator();
            cacheOperator.setInputOperator(inputOperator);
            
            keywordMatcher.setInputOperator(cacheOperator);

            cacheOperator.openAll();
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

            // Update the KeywordMatcher with the new dictionary entry.
            keywordMatcher.close();
            
            keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzer(), predicate.getKeywordMatchingType());
            keywordMatcher = new KeywordMatcher(keywordPredicate);
            keywordMatcher.setInputOperator(cacheOperator);

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
            if (cacheOperator != null) {
                cacheOperator.closeAll();
            }
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }
    
    
    /**
     * The purpose of this operator is to "cache" the tuples from input operator
     *   in an in-memory list.
     * 
     * The DictionaryMatcher uses a new KeywordMatcher for each entry in the dictionary,
     *   each keyword matcher would have to get all the tuples from the input operators again and again.
     * 
     * This operator caches the tuples in the list, so that the tuples don't have to be produced 
     *   from the input operator again and again for multiple keyword mathchers.
     *   
     * This operators relies on a few assumptions in the implementation of DictionaryMatcher:
     *   - At any time, only ONE operator (KeywordMatcher) is connected to this cache operator.
     *   - Multiple operators (KeywordMatchers) are connected to this operator sequentially.
     *   
     * @author Zuozhi Wang
     *
     */
    private class DictionaryTupleCacheOperator implements IOperator {
        
        private IOperator inputOperator;
        private Schema outputSchema;
        
        private ArrayList<ITuple> inputTupleList = new ArrayList<>();
        
        private boolean isOpen = false;
        private boolean inputAllConsumed = false;
        private int cachedTupleCursor = 0;
        
        /*
         * openAll() is the actual "open" function for this cache operator.
         * It will open this operator and its input operator.
         * 
         * It's the caller's responsibility to make sure openAll() is called before everything.
         */
        public void openAll() throws TextDBException {
            if (isOpen) {
                return;
            }
            if (inputOperator == null) {
                throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }
            inputOperator.open();
            outputSchema = inputOperator.getOutputSchema();
            isOpen = true;
        }

        /*
         * When the child operator (KeywordMatcher) calls "open()", we don't want to open the input operator again,
         * so open() is served as an indicator that a new operator (KeywordMatcher) is connected to this operator.
         */
        @Override
        public void open() throws TextDBException {
            if (! isOpen) {
                throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
            }
            // reset the cursor
            cachedTupleCursor = 0;
        }

        @Override
        public ITuple getNextTuple() throws TextDBException {
            if (! isOpen) {
                throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
            }
            // if cursor's next value exceeds the cache's size
            if (cachedTupleCursor + 1 >= inputTupleList.size()) {
                // if the input operator has been fully consumed, return null
                if (inputAllConsumed) {
                    return null;
                // else, get the next tuple from input operator, 
                // add it to tuple list, and advance cursor
                } else {
                    ITuple tuple = inputOperator.getNextTuple();
                    if (tuple == null) {
                        inputAllConsumed = true;
                    } else {
                        inputTupleList.add(tuple);
                        cachedTupleCursor++;
                    }
                    return tuple;
                }
            // if we can get the tuple from the cache, retrieve it and advance cursor
            } else {
                ITuple tuple = inputTupleList.get(cachedTupleCursor);
                cachedTupleCursor++;
                return tuple;
            }
        }
        
        /*
         * closeAll() is the actual "close" function for this cache operator.
         * It will close this operator and its input operator, and clear the cache
         * 
         * It's the caller's responsibility to make sure closeAll() is called after everything.
         */
        public void closeAll() throws TextDBException {
            if (! isOpen) {
                return;
            }
            inputAllConsumed = true;
            isOpen = false;
            cachedTupleCursor = 0;
            inputTupleList = new ArrayList<>();
            inputOperator.close();
        }

        /*
         * When the child operator (KeywordMatcher) calls "close()", we don't want to close the input operator immediately,
         * because there might be some tuples that are still not fetched from input operator.
         * 
         */
        @Override
        public void close() throws TextDBException {
            if (! isOpen) {
                throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
            }
            // reset the cursor
            cachedTupleCursor = 0;
        }
        
        @Override
        public Schema getOutputSchema() {
            return this.outputSchema;
        }
        
        public void setInputOperator(IOperator inputOperator) {
            this.inputOperator = inputOperator;
        }
        
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
