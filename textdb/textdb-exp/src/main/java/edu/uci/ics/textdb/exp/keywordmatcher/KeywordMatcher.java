package edu.uci.ics.textdb.exp.keywordmatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
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
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

public class KeywordMatcher extends AbstractSingleInputOperator {

    private final KeywordPredicate predicate;

    private Schema inputSchema;
    
    private final ArrayList<String> queryTokenList;
    private final HashSet<String> queryTokenSet;
    private final ArrayList<String> queryTokensWithStopwords;

    public KeywordMatcher(KeywordPredicate predicate) {
        this.predicate = predicate;
        
        this.limit = predicate.getLimit();
        this.offset = predicate.getOffset();
        this.queryTokenList = DataflowUtils.tokenizeQuery(predicate.getLuceneAnalyzerString(), predicate.getQuery());
        this.queryTokenSet = new HashSet<>(this.queryTokenList);
        
        // TODO: standard analyzer is assumed here, rewrite it to deal with other analyzers
        this.queryTokensWithStopwords = DataflowUtils.tokenizeQueryWithStopwords(predicate.getQuery());
    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        } else {
            outputSchema = Utils.addAttributeToSchema(outputSchema, 
                    new Attribute(predicate.getSpanListName(), AttributeType.LIST));
        }
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
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
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        // There's an implicit assumption that, in open() method, PAYLOAD is
        // checked before SPAN_LIST.
        // Therefore, PAYLOAD needs to be checked and added first
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(),
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerString()), outputSchema);
        }
        if (predicate.getSpanListName() != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
        }

        List<Span> matchingResults = null;
        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            matchingResults = computeConjunctionMatchingResult(inputTuple);
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            matchingResults = computePhraseMatchingResult(inputTuple);
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
            matchingResults = computeSubstringMatchingResult(inputTuple);
        }
        if (matchingResults == null) {
            throw new DataFlowException("no matching result is provided");
        }
        if (matchingResults.isEmpty()) {
            return null;
        }
        
        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    @Override
    protected void cleanUp() {
    }

    public List<Span> computeConjunctionMatchingResult(Tuple inputTuple) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    Span span = new Span(attributeName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, every token in the query should be present in span
            // list for this field
            if (attributeType == AttributeType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    matchingResults.addAll(fieldSpanList);
                }
            }
        }

        return matchingResults;
    }

    public List<Span> computePhraseMatchingResult(Tuple inputTuple) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    matchingResults.add(new Span(attributeName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue));
                }
            }

            // for TEXT type, spans need to be reconstructed according to the
            // phrase query
            if (attributeType == AttributeType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (!isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    // move on to next field if not all query tokens are present
                    // in the spans
                    continue;
                }

                // Sort current field's span list by token offset for later use
                Collections.sort(fieldSpanList, (span1, span2) -> span1.getTokenOffset() - span2.getTokenOffset());

                List<Integer> queryTokenOffset = new ArrayList<>();

                for (int i = 0; i < queryTokensWithStopwords.size(); i++) {
                    if (queryTokenList.contains(queryTokensWithStopwords.get(i))) {
                        queryTokenOffset.add(i);
                    }
                }

                int iter = 0; // maintains position of term being checked in
                              // spanForThisField list
                while (iter < fieldSpanList.size()) {
                    if (iter > fieldSpanList.size() - queryTokenList.size()) {
                        break;
                    }

                    // Verify if span in the spanForThisField correspond to our
                    // phrase query, ie relative position offsets should be
                    // similar
                    // and the value should be same.
                    boolean isMismatchInSpan = false;// flag to check if a
                                                     // mismatch in spans occurs

                    // To check all the terms in query are verified
                    for (int i = 0; i < queryTokenList.size() - 1; i++) {
                        Span first = fieldSpanList.get(iter + i);
                        Span second = fieldSpanList.get(iter + i + 1);
                        if (!(second.getTokenOffset() - first.getTokenOffset() == queryTokenOffset.get(i + 1)
                                - queryTokenOffset.get(i) && first.getValue().equalsIgnoreCase(queryTokenList.get(i))
                                && second.getValue().equalsIgnoreCase(queryTokenList.get(i + 1)))) {
                            iter++;
                            isMismatchInSpan = true;
                            break;
                        }
                    }

                    if (isMismatchInSpan) {
                        continue;
                    }

                    int combinedSpanStartIndex = fieldSpanList.get(iter).getStart();
                    int combinedSpanEndIndex = fieldSpanList.get(iter + queryTokenList.size() - 1).getEnd();

                    Span combinedSpan = new Span(attributeName, combinedSpanStartIndex, combinedSpanEndIndex, predicate.getQuery(),
                            fieldValue.substring(combinedSpanStartIndex, combinedSpanEndIndex));
                    matchingResults.add(combinedSpan);
                    iter = iter + queryTokenList.size();
                }
            }
        }

        return matchingResults;
    }

    public List<Span> computeSubstringMatchingResult(Tuple inputTuple) throws DataFlowException {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    matchingResults.add(new Span(attributeName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue));
                }
            }

            if (attributeType == AttributeType.TEXT) {
                String regex = predicate.getQuery().toLowerCase();
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();

                    matchingResults.add(new Span(attributeName, start, end, predicate.getQuery(), fieldValue.substring(start, end)));
                }
            }

        }
        return matchingResults;
    }

    private boolean isAllQueryTokensPresent(List<Span> fieldSpanList, Set<String> queryTokenSet) {
        Set<String> fieldSpanKeys = fieldSpanList.stream().map(span -> span.getKey()).collect(Collectors.toSet());

        return fieldSpanKeys.equals(queryTokenSet);
    }

    private List<Span> filterRelevantSpans(List<Span> spanList) {
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

}
