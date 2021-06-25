package edu.uci.ics.texera.dataflow.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

/**
 * @author Qinhua Huang
 */

public class WordCountOperator extends AbstractSingleInputOperator implements ISourceOperator{
    private WordCountOperatorPredicate predicate;
    
    public static final String WORD = "word";
    public static final String COUNT = "count";
    public static final Attribute WORD_ATTR = new Attribute(WORD, AttributeType.STRING);
    public static final Attribute COUNT_ATTR = new Attribute(COUNT, AttributeType.INTEGER);
    public static final Schema SCHEMA_WORD_COUNT = new Schema(SchemaConstants._ID_ATTRIBUTE, WORD_ATTR, COUNT_ATTR);
    
    private List<Entry<String, Integer>> sortedWordCountMap;
    private Iterator<Entry<String, Integer>> wordCountIterator;

    private boolean addPayload = false;
    
    public WordCountOperator(WordCountOperatorPredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataflowException {
        this.outputSchema = SCHEMA_WORD_COUNT;
        this.addPayload = ! inputOperator.getOutputSchema().containsAttribute(SchemaConstants.PAYLOAD);
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        if (sortedWordCountMap == null) {
            computeWordCount();
        }
        
        if (wordCountIterator.hasNext()) {
            Entry<String, Integer> entry = wordCountIterator.next();
            List<IField> tupleFieldList = new ArrayList<>();
            // Generate the new UUID.
            tupleFieldList.add(IDField.newRandomID());
            tupleFieldList.add(new StringField(entry.getKey()));
            tupleFieldList.add(new IntegerField(entry.getValue()));
            
            return new Tuple(outputSchema, tupleFieldList);
            
        }
        
        return null;
    }
    
    private void computeWordCount() throws TexeraException {
        Tuple tuple;
        HashMap<String, Integer> wordCountMap = new HashMap<>();
        while ((tuple = this.inputOperator.getNextTuple()) != null) {
            if (addPayload) {
                tuple = new Tuple.Builder(tuple).add(SchemaConstants.PAYLOAD_ATTRIBUTE,new ListField<Span>(
                                DataflowUtils.generatePayloadFromTuple(tuple, predicate.getLuceneAnalyzerString()))).build();
            }
            
            ListField<Span> payloadField = tuple.getField("payload");
            List<Span> payloadSpanList = payloadField.getValue();
            
            for (Span span : payloadSpanList) {
                if (span.getAttributeName().equals(predicate.getAttribute())) {
                    String key = span.getValue().toLowerCase();
                    wordCountMap.put(key, wordCountMap.get(key)==null ? 1 : wordCountMap.get(key) + 1);
                }
            }
        }
        sortedWordCountMap = wordCountMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .collect(Collectors.toList());
        wordCountIterator = sortedWordCountMap.iterator();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void cleanUp() throws TexeraException {
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
