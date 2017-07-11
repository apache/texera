package edu.uci.ics.textdb.exp.nltk;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class Nltk implements IOperator {

	private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public Nltk(){
    	
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    private Schema transformSchema(Schema inputSchema){
    	
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute("NLTK Result", AttributeType.STRING));
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
     // generate output schema by transforming the input schema
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        
        cursor = OPENED;
		
	}
	
	
	public static void main(String[] args2){
		try{
			StringBuilder str = new StringBuilder();
			String path = Utils.getResourcePath("Python", TextdbProject.TEXTDB_EXP);
			path += "/script.py";
			System.out.println(path);
			String istr =  "@MSNBC @EllicottCity @weatherchannel #Climate Change @SpeakerRyan the atmosphere has4% more water than before the INDUSTRIAL AGE.";
			for(int i=0;i<5;i++){
				
				List<String> args = new ArrayList<String>(Arrays.asList("python3",path));
				args.addAll(new ArrayList<String>(Arrays.asList(istr)));
				
				ProcessBuilder pb = new ProcessBuilder(args);
				
				Process p = pb.start();
				int exitcode = p.waitFor();
				
				BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String s = null;
				while ((s = input.readLine()) != null) {
//				    System.out.println(s);
				   	str.append(s);
				}
				
				input.close();
				System.out.println("this"+new StringField(str.toString()).getValue());
			}
			
			
			
		}catch(Exception e){
			e.printStackTrace();
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
        String str = "";
        try{
        	String path = Utils.getResourcePath("Python", TextdbProject.TEXTDB_EXP);
			path += "/script.py";
//			System.out.println(path);
			List<String> args = new ArrayList<String>(Arrays.asList("python3",path));
			args.addAll(new ArrayList<String>(Arrays.asList(inputTuple.getField("text").toString())));
			
			ProcessBuilder pb = new ProcessBuilder(args);
			
			Process p = pb.start();
			int exitcode = p.waitFor();
			
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s = null;
			while ((s = input.readLine()) != null) {
			    
			    str += s;
			}
			
			input.close();
        }catch(Exception e){
        	e.printStackTrace();
        	str += e.getMessage();
        }
        System.out.println(str);
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(inputTuple.getFields());
        
        outputFields.add(new StringField(str));
        
        return new Tuple(outputSchema, outputFields);
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
