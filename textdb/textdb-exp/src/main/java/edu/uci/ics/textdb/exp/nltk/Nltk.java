package edu.uci.ics.textdb.exp.nltk;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class Nltk implements IOperator {
    private final NltkPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public static String PYTHONSCRIPT = "/classifier-loader.py";
    
    public Nltk(NltkPredicate predicate){
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * adds a new field to the schema, with name resultAttributeName and type String
     */
    private Schema transformSchema(Schema inputSchema){
        if (inputSchema.containsField(predicate.getResultAttributeName())) {
            throw new RuntimeException(String.format(
                    "result attribute name %s is already in the original schema %s", 
                    predicate.getResultAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute(predicate.getResultAttributeName(), AttributeType.STRING));
    }
    
	@Override
	public void open() throws TextDBException {
	    if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
        
        // check if input schema is present
        if (! inputSchema.containsField(predicate.getInputAttributeName())) {
            throw new RuntimeException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        
        // check if attribute type is valid
        AttributeType inputAttributeType = 
                inputSchema.getAttribute(predicate.getInputAttributeName()).getAttributeType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) || 
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new RuntimeException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }
        
        // generate output schema by transforming the input schema
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        
        cursor = OPENED;
	}
	
	public static void main(String[] args2){
	    String className;
	    String inputText = "I am glad you arrived!! :)";
        
        try{
            String path = Utils.getResourcePath("Python", TextdbProject.TEXTDB_EXP);
            path += PYTHONSCRIPT;
            List<String> args = new ArrayList<String>(Arrays.asList("python3",path));
            args.addAll(new ArrayList<String>(Arrays.asList(inputText)));
            
            ProcessBuilder pb = new ProcessBuilder(args);
            
            Process p = pb.start();
            int exitcode = p.waitFor();
            
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            if ((className = input.readLine()) != null) {
                input.close();
                System.out.println(className);
            }
        } catch(Exception e){
            e.printStackTrace();
            System.out.print(e.getMessage());
        }
	}
	@Override
	public Tuple getNextTuple() throws TextDBException {
	    if (cursor == CLOSED) {
            return null;
        }
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(inputTuple.getFields());
        outputFields.add(new TextField(computeClassLabel(inputTuple)));
        
        return new Tuple(outputSchema, outputFields);
        
	}

	private String computeClassLabel(Tuple inputTuple) {
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        String className;
        
        try{
            String path = Utils.getResourcePath("Python", TextdbProject.TEXTDB_EXP);
            path += PYTHONSCRIPT;
            List<String> args = new ArrayList<String>(Arrays.asList("python3",path));
            args.addAll(new ArrayList<String>(Arrays.asList(inputText)));
            
            ProcessBuilder pb = new ProcessBuilder(args);
            
            Process p = pb.start();
            int exitcode = p.waitFor();
            
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            if ((className = input.readLine()) != null) {
                input.close();
                return className;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
	
	
	@Override
	public void close() throws TextDBException {
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
	
}
