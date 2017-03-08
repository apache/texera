package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;

public class KeywordMatcher extends AbstractSingleInputOperator {

    private KeywordPredicate predicate;

    private Schema inputSchema;

    public KeywordMatcher(KeywordPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
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
        Tuple resultTuple = null;

        // There's an implicit assumption that, in open() method, PAYLOAD is
        // checked before SPAN_LIST.
        // Therefore, PAYLOAD needs to be checked and added first
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            inputTuple = Utils.getSpanTuple(inputTuple.getFields(),
                    Utils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzer()), outputSchema);
        }
        if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            inputTuple = Utils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
        }

        if (this.predicate.getOperatorType() == DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            resultTuple = computeConjunctionMatchingResult(inputTuple);
        }
        if (this.predicate.getOperatorType() == DataConstants.KeywordMatchingType.PHRASE_INDEXBASED) {
            resultTuple = computePhraseMatchingResult(inputTuple);
        }
        if (this.predicate.getOperatorType() == DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED) {
            resultTuple = computeSubstringMatchingResult(inputTuple);
        }

        return resultTuple;
    }

    @Override
    protected void cleanUp() {
    }

    private Tuple computeConjunctionMatchingResult(Tuple sourceTuple) throws DataFlowException {
        ListField<Span> payloadField = sourceTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchingResults = new ArrayList<>();

        for (String fieldName : this.predicate.getAttributeNames()) {
            FieldType fieldType = this.inputSchema.getAttribute(fieldName).getFieldType();
            String fieldValue = sourceTuple.getField(fieldName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    Span span = new Span(fieldName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, every token in the query should be present in span
            // list for this field
            if (fieldType == FieldType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getFieldName().equals(fieldName))
                        .collect(Collectors.toList());

                if (isAllQueryTokensPresent(fieldSpanList, predicate.getQueryTokenSet())) {
                    matchingResults.addAll(fieldSpanList);
                }
            }

        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = sourceTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return sourceTuple;
    }

    private Tuple computePhraseMatchingResult(Tuple sourceTuple) throws DataFlowException {
        ListField<Span> payloadField = sourceTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchingResults = new ArrayList<>();

        for (String fieldName : this.predicate.getAttributeNames()) {
            FieldType fieldType = this.inputSchema.getAttribute(fieldName).getFieldType();
            String fieldValue = sourceTuple.getField(fieldName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    matchingResults.add(new Span(fieldName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue));
                }
            }

            // for TEXT type, spans need to be reconstructed according to the
            // phrase query
            if (fieldType == FieldType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getFieldName().equals(fieldName))
                        .collect(Collectors.toList());

                if (!isAllQueryTokensPresent(fieldSpanList, predicate.getQueryTokenSet())) {
                    // move on to next field if not all query tokens are present
                    // in the spans
                    continue;
                }

                // Sort current field's span list by token offset for later use
                Collections.sort(fieldSpanList, (span1, span2) -> span1.getTokenOffset() - span2.getTokenOffset());

                List<String> queryTokenList = predicate.getQueryTokenList();
                List<Integer> queryTokenOffset = new ArrayList<>();
                List<String> queryTokensWithStopwords = predicate.getQueryTokensWithStopwords();

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

                    Span combinedSpan = new Span(fieldName, combinedSpanStartIndex, combinedSpanEndIndex, predicate.getQuery(),
                            fieldValue.substring(combinedSpanStartIndex, combinedSpanEndIndex));
                    matchingResults.add(combinedSpan);
                    iter = iter + queryTokenList.size();
                }
            }
        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = sourceTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return sourceTuple;
    }

    private Tuple computeSubstringMatchingResult(Tuple sourceTuple) throws DataFlowException {
        List<Span> matchingResults = new ArrayList<>();

        for (String fieldName : this.predicate.getAttributeNames()) {
            FieldType fieldType = this.inputSchema.getAttribute(fieldName).getFieldType();
            String fieldValue = sourceTuple.getField(fieldName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(predicate.getQuery())) {
                    matchingResults.add(new Span(fieldName, 0, predicate.getQuery().length(), predicate.getQuery(), fieldValue));
                }
            }

            if (fieldType == FieldType.TEXT) {
                String regex = predicate.getQuery().toLowerCase();
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();

                    matchingResults.add(new Span(fieldName, start, end, predicate.getQuery(), fieldValue.substring(start, end)));
                }
            }

        }
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = sourceTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return sourceTuple;
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
            if (predicate.getQueryTokenSet().contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
    }

    public KeywordPredicate getPredicate() {
        return this.predicate;
    }

}
