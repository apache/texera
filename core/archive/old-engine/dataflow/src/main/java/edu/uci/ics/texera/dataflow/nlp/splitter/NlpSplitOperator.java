package edu.uci.ics.texera.dataflow.nlp.splitter;

import java.util.ArrayList;
import java.util.List;
import java.io.Reader;
import java.io.StringReader;

import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.ling.HasWord;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * This Operator splits the input string using Stanford core NLP's ssplit annotation.
 * 
 * The result is an list of sentences.
 * 
 * The result will be put into an attribute with resultAttributeName specified in predicate, and type List<String>.
 * 
 * @author Venkata Raj Kiran Kollimarla, Vinay Bagade
 *
 */
public class NlpSplitOperator implements IOperator {
    
    private final NlpSplitPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    //A flag to keep track of any remaining sentences from the previous input tuple
    //A tuple that persists between method calls
    private Tuple currentTuple;
    //A list of sentences generated from the current tuple
    private List<Span> currentSentenceList = new ArrayList<Span>();
    
    
    public NlpSplitOperator(NlpSplitPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) throws DataflowException {
        if (cursor != CLOSED) {  
            throw new DataflowException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        
        Schema inputSchema = inputOperator.getOutputSchema();
        // generate output schema by transforming the input schema based on what output format
        // is chosen (OneToOne vs. OneToMany)
        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());
        
        // check if attribute type is valid
        AttributeType inputAttributeType =
                inputSchema.getAttribute(predicate.getInputAttributeName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) || 
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new DataflowException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }
        
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        
        if(predicate.getOutputType() == NLPOutputType.ONE_TO_ONE) {
            currentTuple = inputOperator.getNextTuple();
            if (currentTuple == null) return null;
            outputFields.addAll(currentTuple.getFields());
            outputFields.add(new ListField<Span>(computeSentenceList(currentTuple)));
        }
        
        else if (predicate.getOutputType() == NLPOutputType.ONE_TO_MANY) {
            if(currentSentenceList.isEmpty()) {
                currentTuple = inputOperator.getNextTuple();
                if (currentTuple == null) return null;
                currentSentenceList = computeSentenceList(currentTuple);
            }
            
            //Add new ID for each new tuple created
            outputFields.add(IDField.newRandomID());
            //Skip the ID field in the input tuple
            for (String attributeName : currentTuple.getSchema().getAttributeNames()) {
                if(!attributeName.equals(SchemaConstants._ID))
                    outputFields.add(currentTuple.getField(attributeName));
            }
            
            //Add the sentences from the current sentence list one by one in the order in which
            //they were generated in the getSentenceList function, append a TextField to the output 
            // tuple and add the string contained in the current span
            outputFields.add(new TextField(currentSentenceList.remove(0).getValue()));    
        }
        
        return new Tuple(outputSchema, outputFields);
    }
    
    
    private List<Span> computeSentenceList(Tuple inputTuple) {
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        Reader reader = new StringReader(inputText);
        DocumentPreprocessor documentPreprocessor = new DocumentPreprocessor(reader);
        documentPreprocessor.setTokenizerFactory(PTBTokenizer.PTBTokenizerFactory.newCoreLabelTokenizerFactory("ptb3Escaping=false"));
        List<Span> sentenceList = new ArrayList<Span>();
        
        int start = 0; int end = 0; 
        String key=PropertyNameConstants.NLP_SPLIT_KEY;
        String attributeName = predicate.getInputAttributeName();
        for (List<HasWord> sentence : documentPreprocessor) {
            String sentenceText = SentenceUtils.listToString(sentence);
            //Make span
            end = start + sentenceText.length(); 
            Span span = new Span(attributeName, start, end, key, sentenceText);
            sentenceList.add(span);
            start = end + 1;
         }
        
        return sentenceList;
    }
    

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }


    /*
     * adds a new field to the schema, with name resultAttributeName and type list of strings
     */
    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.checkAttributeExists(inputSchema[0], predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema[0], predicate.getResultAttributeName());

        if (predicate.getOutputType() == NLPOutputType.ONE_TO_ONE)
            return new Schema.Builder().add(inputSchema[0]).add(predicate.getResultAttributeName(), AttributeType.LIST).build();
        else
            return new Schema.Builder().add(inputSchema[0]).add(predicate.getResultAttributeName(), AttributeType.TEXT).build();
    }
}
