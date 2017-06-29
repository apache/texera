package edu.uci.ics.textdb.exp.dictionarymatcher;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatcher;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordPredicate;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Chang on 6/28/17.
 */
public class DictionaryMatcher extends AbstractSingleInputOperator {
    private final DictionaryPredicate predicate;
    private int limit;
    private int offset;
    String currentDictionaryEntry;
    private KeywordPredicate keywordPredicate;
    private KeywordMatcher keywordMatcher;

    public DictionaryMatcher(DictionaryPredicate predicate) {
        this.predicate = predicate;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;

    }

    private Schema inputSchema;

    @Override
    protected void setUp() throws TextDBException {
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputSchema = inputOperator.getOutputSchema();
        predicate.getDictionary().resetCursor();
        //currentDictionaryEntry = predicate.getDictionary().getNextEntry();

        if (predicate.getDictionary().isEmpty()) {
            throw new DataFlowException("Dictionary is empty");
        }
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = Utils.addAttributeToSchema(inputSchema,
                new Attribute(predicate.getSpanListName(), AttributeType.LIST));

    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }

        return resultTuple;

    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        if (inputTuple == null) {
            return null;
        }
        Dictionary dictionary = new Dictionary(predicate.getDictionary().getDictionaryEntries());
        List<Span> matchingResults = new ArrayList<>();
       // currentDictionaryEntry = dictionary.getNextEntry();
        while ((currentDictionaryEntry = dictionary.getNextEntry()) != null) {
            if(predicate.getKeywordMatchingType()== KeywordMatchingType.SUBSTRING_SCANBASED){
                matchingResults.addAll(matchOneKeywordSubstring(inputTuple, currentDictionaryEntry));
            }
            else if(predicate.getKeywordMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED){
                /*ToDO*/
            }
            else if(predicate.getKeywordMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED){
                /*TODO*/
            }

        }


//            keywordPredicate = new KeywordPredicate(currentDictionaryEntry,
//                    predicate.getAttributeNames(),
//                    predicate.getAnalyzerString(), predicate.getKeywordMatchingType(),
//                    predicate.getSpanListName());
//            keywordMatcher = new KeywordMatcher(keywordPredicate);
        // if (this.predicate.getKeywordMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
        //    matchingResults.addAll(keywordMatcher.computeConjunctionMatchingResult(inputTuple));
        // }
        // else if (this.predicate.getKeywordMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
        //     matchingResults = keywordMatcher.computePhraseMatchingResult(inputTuple);
        // }
        //else if (this.predicate.getKeywordMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
        //     matchingResults = keywordMatcher.computeSubstringMatchingResult(inputTuple);
        // }

        //keywordMatcher.setUp();
        //   matchingResults.addAll((List<Span>) keywordMatcher.processOneInputTuple(inputTuple).getField(predicate.getSpanListName()).getValue());
        //    currentDictionaryEntry=predicate.getDictionary().getNextEntry();

        if (matchingResults.isEmpty()) {
            return null;
        }
        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;

    }

    private List<Span> matchOneKeywordSubstring(Tuple inputTuple, String currentDictionaryEntry) throws DataFlowException {
        List<Span> matchingResult = new ArrayList<>();
        for (String attribute : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attribute).getAttributeType();
            String fieldValue = inputTuple.getField(attribute).getValue().toString();

            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }


            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(currentDictionaryEntry)) {
                    matchingResult.add(new Span(attribute, 0, currentDictionaryEntry.length(), currentDictionaryEntry, fieldValue));
                }
            }

            if (attributeType == AttributeType.TEXT) {
                String regex = currentDictionaryEntry.toLowerCase();
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();

                    matchingResult.add(new Span(attribute, start, end, currentDictionaryEntry, fieldValue.substring(start, end)));
                }
            }
        }
        return matchingResult;
    }


    @Override
    protected void cleanUp() throws TextDBException {

    }

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
