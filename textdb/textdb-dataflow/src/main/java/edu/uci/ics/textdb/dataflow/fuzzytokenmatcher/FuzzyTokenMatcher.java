package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;

/**
 *  @author Zuozhi Wang (zuozhiw)
 *  @author Parag Saraogi
 *  @author Varun Bharill
 *  
 *  This class provides token based fuzzy matching.
 */
public class FuzzyTokenMatcher extends AbstractSingleInputOperator {
    
    private FuzzyTokenPredicate predicate;
    private int threshold;
    private ArrayList<String> queryTokens;
    
    private Schema inputSchema;
    
    public FuzzyTokenMatcher(FuzzyTokenPredicate predicate) {
        this.predicate = predicate;
        this.threshold = predicate.getThreshold();
        this.queryTokens = predicate.getQueryTokens();
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
    protected ITuple computeNextMatchingTuple() throws TextDBException {
        ITuple inputTuple = null;
        ITuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {          
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
            
            resultTuple = processOneInputTuple(inputTuple);
            
            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws TextDBException {
        List<Span> payload = (List<Span>) inputTuple.getField(SchemaConstants.PAYLOAD).getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchResults = new ArrayList<>();

        /*
         * The source operator returns spans even for those fields which did not
         * satisfy the threshold criterion. So if two attributes A,B have 10 and
         * 5 matching tokens, and we set threshold to 10, the number of spans
         * returned is 15. So we need to filter those 5 spans for attribute B.
         */
        for (Attribute attribute : this.predicate.getAttributeList()) {
            String fieldName = attribute.getFieldName();
            FieldType fieldType = attribute.getFieldType();            
            
            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.TEXT) {
                throw new DataFlowException("FuzzyTokenMatcher: Fields other than TEXT are not supported");
            }
            
            List<Span> fieldSpans = 
                    relevantSpans.stream()
                    .filter(span -> span.getFieldName().equals(fieldName))
                    .filter(span -> queryTokens.contains(span.getKey()))
                    .collect(Collectors.toList());
            
            if (fieldSpans.size() >= threshold) {
                matchResults.addAll(fieldSpans);
            }          
        }

        if (matchResults.isEmpty()) {
            return null;
        }

        List<Span> spanList = (List<Span>) inputTuple.getField(SchemaConstants.SPAN_LIST).getValue();
        spanList.addAll(matchResults);

        return inputTuple;
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

    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public FuzzyTokenPredicate getPredicate() {
        return this.predicate;
    }

}
