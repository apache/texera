package edu.uci.ics.texera.dataflow.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

/**
 *  @author Zuozhi Wang (zuozhiw)
 *  @author Parag Saraogi
 *  @author Varun Bharill
 *  
 *  This class provides token based fuzzy matching.
 */
public class FuzzyTokenMatcher extends AbstractSingleInputOperator {
    
    private final FuzzyTokenPredicate predicate;
    
    private Schema inputSchema;
    
    private boolean addPayload = false;
    private boolean addResultAttribute = false;
    
    public FuzzyTokenMatcher(FuzzyTokenPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        
        this.addPayload = ! inputSchema.containsAttribute(SchemaConstants.PAYLOAD);
        this.addResultAttribute = predicate.getSpanListName() != null;
        
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeNames());
        if (addResultAttribute) {
            Schema.checkAttributeNotExists(inputSchema, predicate.getSpanListName());
        }

        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());
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
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerStr())));
            inputTuple = tupleBuilderPayload.build();
        }
        
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> relevantSpans = filterRelevantSpans(payloadField.getValue());
        List<Span> matchingResults = new ArrayList<>();

        /*
         * The source operator returns spans even for those fields which did not
         * satisfy the threshold criterion. So if two attributes A,B have 10 and
         * 5 matching tokens, and we set threshold to 10, the number of spans
         * returned is 15. So we need to filter those 5 spans for attribute B.
         */
        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getType();
            
            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.TEXT && attributeType != AttributeType.STRING) {
                throw new DataflowException("FuzzyTokenMatcher: Fields other than TEXT or STRING are not supported");
            }
            
            List<Span> fieldSpans = 
                    relevantSpans.stream()
                    .filter(span -> span.getAttributeName().equals(attributeName))
                    .filter(span -> predicate.getQueryTokens().contains(span.getKey()))
                    .collect(Collectors.toList());
            
            if (fieldSpans.size() >= predicate.getThreshold()) {
                matchingResults.addAll(fieldSpans);
            }
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
    
    private List<Span> filterRelevantSpans(List<Span> spanList) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span = iterator.next();
            if (predicate.getQueryTokens().contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
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

    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public FuzzyTokenPredicate getPredicate() {
        return this.predicate;
    }

}
