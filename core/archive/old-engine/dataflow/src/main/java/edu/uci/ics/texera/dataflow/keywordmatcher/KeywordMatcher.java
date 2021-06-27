package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

public class KeywordMatcher extends AbstractSingleInputOperator {

    private final KeywordPredicate predicate;

    private Schema inputSchema;
    private Set<String> queryTokenSet;
    private ArrayList<String> queryTokenList;
    private ArrayList<String> queryTokenWithStopwordsList;
    
    private boolean addPayload = false;
    private boolean addResultAttribute = false;

    public KeywordMatcher(KeywordPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        
        inputSchema = inputOperator.getOutputSchema();
        
        this.addPayload = ! inputSchema.containsAttribute(SchemaConstants.PAYLOAD);
        this.addResultAttribute = predicate.getSpanListName() != null;
        
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeNames());
        if (addResultAttribute) {
            Schema.checkAttributeNotExists(inputSchema, predicate.getSpanListName());
        }

        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());
        
        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            preProcessKeywordTokens();
        } else if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            preProcessKeywordTokensWithStopwords();
        }
    }

    private void preProcessKeywordTokens() {
        queryTokenSet = new HashSet<>(DataflowUtils.tokenizeQuery(predicate.getLuceneAnalyzerString(), predicate.getQuery()));
    }

    private void preProcessKeywordTokensWithStopwords() {
        queryTokenList = DataflowUtils.tokenizeQuery(predicate.getLuceneAnalyzerString(), predicate.getQuery());
        queryTokenWithStopwordsList = DataflowUtils.tokenizeQueryWithStopwords(
                predicate.getLuceneAnalyzerString(), predicate.getQuery());
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            resultTuple = processOneInputTuple(inputTuple);

            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        // add payload if needed before passing it to the matching functions
        if (addPayload) {
            Tuple.Builder tupleBuilderPayload = new Tuple.Builder(inputTuple);
            tupleBuilderPayload.add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerString())));
            inputTuple = tupleBuilderPayload.build();
        }
        
        // compute the keyword matching results
        List<Span> matchingResults = null;
        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
           matchingResults =  appendConjunctionMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenSet, predicate.getQuery());
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            matchingResults = appendPhraseMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenList, queryTokenWithStopwordsList, predicate.getQuery());
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
            matchingResults = appendSubstringMatchingSpans(inputTuple, predicate.getAttributeNames(), predicate.getQuery());
        }
        
        if (matchingResults.isEmpty()) {
            return null;
        }
        
        Tuple.Builder tupleBuilder = new Tuple.Builder(inputTuple);
        if (addResultAttribute) {
            tupleBuilder.add(predicate.getSpanListName(), AttributeType.LIST, new ListField<Span>(matchingResults));
        }
        return tupleBuilder.build();
    }

    @Override
    protected void cleanUp() {
    }

    private List<Span> appendPhraseMatchingSpans(Tuple inputTuple, List<String> attributeNames, List<String> queryTokenList, List<String> queryTokenListWithStopwords, String queryKeyword) throws DataflowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (queryKeyword.equals(fieldValue)) {
                    Span span = new Span(attributeName, 0, fieldValue.length(), fieldValue, fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, spans need to be reconstructed according to the
            // phrase query
            if (attributeType == AttributeType.TEXT) {
                Set<String> queryTokenSet = new HashSet<>(queryTokenList);
                List<Span> relevantSpans = filterRelevantSpans(payload, queryTokenSet);
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (!DataflowUtils.isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    // move on to next field if not all query tokens are present
                    // in the spans
                    continue;
                }
                matchingResults.addAll(DataflowUtils.constructPhraseMatchingSpans(attributeName, fieldValue, queryKeyword, fieldSpanList, queryTokenListWithStopwords, queryTokenList));
            }
        }
        return matchingResults;
    }

    private List<Span> appendConjunctionMatchingSpans(Tuple inputTuple, List<String> attributeNames, Set<String> queryTokenSet, String queryKeyword) throws DataflowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (queryKeyword.equals(fieldValue)) {
                    Span span = new Span(attributeName, 0, fieldValue.length(), fieldValue, fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, every token in the query should be present in span
            // list for this field
            if (attributeType == AttributeType.TEXT) {
                List<Span> relevantSpans = filterRelevantSpans(payload, queryTokenSet);
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());
                if (DataflowUtils.isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    matchingResults.addAll(fieldSpanList);
                }

            }
        }
        return matchingResults;
    }

    private List<Span> appendSubstringMatchingSpans(Tuple inputTuple, List<String> attributeNames, String queryKeyword) throws DataflowException {
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : attributeNames) {
            //  AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(queryKeyword)) {
                    matchingResults.add(new Span(attributeName, 0, queryKeyword.length(), queryKeyword, fieldValue));
                }
            }

            if (attributeType == AttributeType.TEXT) {

                String fieldValueLowerCase = fieldValue.toLowerCase();
                String queryKeywordLowerCase = queryKeyword.toLowerCase();
                for (int i = 0; i < fieldValueLowerCase.length(); i++) {
                    int index = -1;
                    if ((index = fieldValueLowerCase.indexOf(queryKeywordLowerCase, i)) != -1) {
                        matchingResults.add(new Span(attributeName, index, index + queryKeyword.length(), queryKeyword,
                                fieldValue.substring(index, index + queryKeyword.length())));
                        i = index + 1;
                    } else {
                        break;
                    }
                }

            }
        }
        return matchingResults;
    }
    
    private List<Span> filterRelevantSpans(List<Span> spanList, Set<String> queryTokenSet) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span = iterator.next();
            if (queryTokenSet.contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
    }

    public KeywordPredicate getPredicate() {
        return this.predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.Builder outputSchemaBuilder = new Schema.Builder(inputSchema[0]);
        if (addPayload) {
            outputSchemaBuilder.add(SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (addResultAttribute) {
            outputSchemaBuilder.add(predicate.getSpanListName(), AttributeType.LIST);
        }
        return outputSchemaBuilder.build();
    }

}
