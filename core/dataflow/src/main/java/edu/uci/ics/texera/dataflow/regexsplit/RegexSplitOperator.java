package edu.uci.ics.texera.dataflow.regexsplit;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 * @author Zuozhi Wang
 * 
 * This class is to divide an attribute of TextField or StringField by using a regex in order to get multiple tuples, 
 * with the other attributes unchanged.
 * 
 * For example:
 * Source text: "Make America be Great America Again."
 * splitRegex = "America"
 * Split output can be one of the following three based on the SplitTypes:
 * 1. GROUP_RIGHT: ["Make ", "America be Great ", "America Again."]
 * 2. GROUP_LEFT: ["Make America", " be Great America"," Again."]
 * 3. STANDALONE: ["Make ", "America", " be Great ", "America", " Again."]
 * 
 * When a string contains multiple repeated patterns, this operator will only return the longest pattern.
 * For example:
 * Text = "banana"
 * regex = "b.*ana";
 * result list = <"banana">
 * 
 * If the old tuple has an ID field, remove it.
 */
public class RegexSplitOperator extends AbstractSingleInputOperator implements ISourceOperator{

    private RegexSplitPredicate predicate;
    Tuple currentTuple;
    
    private List<Span> currentSentenceList = new ArrayList<Span>();

    public RegexSplitOperator(RegexSplitPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataflowException {
        Schema inputSchema = inputOperator.getOutputSchema();
        // generate output schema by transforming the input schema based on what output format
        // is chosen (OneToOne vs. OneToMany)
        this.outputSchema = transformToOutputSchema(inputSchema);
        
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
        

    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        
        List<IField> outputFields = new ArrayList<>();
        
        if(predicate.getOutputType() == RegexOutputType.ONE_TO_ONE) {
            currentTuple = inputOperator.getNextTuple();
            if (currentTuple == null) 
                return null;
            outputFields.addAll(currentTuple.getFields());
            outputFields.add(new ListField<Span>(computeSentenceList(currentTuple)));
        } else if(predicate.getOutputType() == RegexOutputType.ONE_TO_MANY) {
            if(currentSentenceList.isEmpty()) {
                currentTuple = inputOperator.getNextTuple();
                if (currentTuple == null) 
                    return null;
                currentSentenceList = computeSentenceList(currentTuple);
            }
            
            //Add new ID for each new tuple created
            outputFields.add(IDField.newRandomID());
            //Skip the ID field in the input tuple
            for (String attributeName : currentTuple.getSchema().getAttributeNames()) {
                if(!attributeName.equals(SchemaConstants._ID)) {
                    outputFields.add(currentTuple.getField(attributeName));
                }
            }
            
            //Add the sentences from the current sentence list one by one in the order in which
            //they were generated in the getSentenceList function, append a TextField to the output 
            // tuple and add the string contained in the current span
            String tmpStr = currentSentenceList.remove(0).getValue();
                    
            outputFields.add(new TextField(tmpStr));
        }
        return new Tuple(outputSchema, outputFields);
    }
    
    private List<Span> computeSentenceList(Tuple inputTuple) {
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        List<Span> textSpanList = new ArrayList<Span>();
        
        String attributeName = predicate.getInputAttributeName();
        
        //Create a pattern using regex.
        Pattern pattern = Pattern.compile(predicate.getRegex());
        
        // Match the pattern in the text.
        Matcher regexMatcher = pattern.matcher(inputText);
        List<Integer> splitIndex = new ArrayList<Integer>();
        splitIndex.add(0);
        int endSplit;
        int startSplit;
        while(regexMatcher.find()) {
            if (predicate.getSplitType() == RegexSplitPredicate.SplitType.GROUP_RIGHT) {
                endSplit = regexMatcher.start();
                startSplit = endSplit;
                if (startSplit != 0) {
                    splitIndex.add(endSplit);
                    splitIndex.add(startSplit);
                }
            } else if (predicate.getSplitType() == RegexSplitPredicate.SplitType.GROUP_LEFT) {
                endSplit = regexMatcher.end();
                startSplit = endSplit;
                
                splitIndex.add(endSplit);
                splitIndex.add(startSplit);
                
            } else if (predicate.getSplitType() == RegexSplitPredicate.SplitType.STANDALONE) {
                endSplit = regexMatcher.start();
                startSplit = endSplit;
                if (endSplit != 0) {
                    splitIndex.add(endSplit);
                    splitIndex.add(startSplit);
                }
                
                endSplit = regexMatcher.end();
                startSplit = endSplit;
                if (endSplit < inputText.length() ) {
                    splitIndex.add(endSplit); splitIndex.add(startSplit);
                }
            }
        }
        splitIndex.add(inputText.length());
        
        //Make span list
        int startSpan = 0;
        int endSpan = 0; 
        String key=PropertyNameConstants.REGEX_SPLIT_KEY;
        for (int i = 0 ; i < splitIndex.size() - 1; i++) {
            if (splitIndex.get(i) <= splitIndex.get(i+1)) {
                String textSpan = inputText.substring(splitIndex.get(i),splitIndex.get(i+1));
                startSpan = splitIndex.get(i);
                i++;
                endSpan = startSpan + textSpan.length();
                Span span = new Span(attributeName, startSpan, endSpan, key, textSpan);
                textSpanList.add(span);
            }
        }
        
        return textSpanList;
    }
    
    @Override
    protected void cleanUp() throws TexeraException {
    }
    
    public RegexSplitPredicate getPredicate() {
        return this.predicate;
    }
    
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        throw new TexeraException("RegexSplit does not support process one tuple");
    }


    /*
     * adds a new field to the schema, with name resultAttributeName and type list of strings
     */
    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {

        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.checkAttributeExists(inputSchema[0], predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema[0], predicate.getResultAttributeName());

        if (predicate.getOutputType() == RegexOutputType.ONE_TO_ONE)
            return new Schema.Builder().add(inputSchema[0]).add(predicate.getResultAttributeName(), AttributeType.LIST).build();
        else
            return new Schema.Builder().add(inputSchema[0]).add(predicate.getResultAttributeName(), AttributeType.TEXT).build();
    }
}