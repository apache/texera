package edu.uci.ics.texera.dataflow.sink.json;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;

public class JSONSink implements ISink {

    private JSONSinkPredicate predicate;
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;

    private ObjectMapper mapper;
    private JsonGenerator jsonGenerator;
    private int cursor = CLOSED;
    
    private Path jsonIndexDirectory = Utils.getDefaultIndexDirectory().resolve("json");
    private String fileName;

    
    public JSONSink(JSONSinkPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void open() throws TexeraException{
        if (cursor != CLOSED) {
            return;
        }
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema(inputSchema.getAttributes().stream()
                .filter(attr -> ! attr.getName().equalsIgnoreCase(SchemaConstants._ID))
                .filter(attr -> ! attr.getName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
                .toArray(Attribute[]::new));
        
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        fileName = df.format(new Date()) + ".json";
        
        mapper = new ObjectMapper();
        File file = new File(jsonIndexDirectory.resolve(fileName).toString()); 

    	try {
    	    if (Files.notExists(jsonIndexDirectory)) {
    	        Files.createDirectories(jsonIndexDirectory);
    	    }
    	    // creates json generator factory for writing to file
    	    jsonGenerator = mapper.getFactory().createGenerator(file, JsonEncoding.UTF8);
        	jsonGenerator.writeStartArray();

    	} catch (IOException e) {
			throw new DataflowException(e);
		}
    	
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        return;
    }
    
    /**
     * getNextTuple() will return the cleaned tuple from previous inputOperator.
     * At the same time, it creates a new row in json and writes the cleaned tuple to that row.
     * List field is kept as one single string.
     */
    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }        
        if (cursor >= predicate.getLimit() + predicate.getOffset()) {
            return null;
        }
        Tuple inputTuple = null;
        while (true) {
            inputTuple = inputOperator.getNextTuple();
            if (inputTuple == null) {
                return null;
            }
            cursor++;
            if (cursor > predicate.getOffset()) {
                break;
            }
        }
        
        Tuple resultTuple = new Tuple.Builder(inputTuple).removeIfExists(SchemaConstants._ID, SchemaConstants.PAYLOAD).build();
        
        try {
        	// for each row, it represents a single object in json format
			jsonGenerator.writeStartObject();
	        for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {
	        	writeField(outputSchema.getAttributeNames().get(i), 
	        			resultTuple.getField(outputSchema.getAttributeNames().get(i)));
	        }
	        jsonGenerator.writeEndObject();
		} catch (IOException e) {
			throw new DataflowException(e);
		}

        return resultTuple;
    }
    
    /*
     * Write a field into a cell base on the field type.
     * 		Excel-Double <-> DoubleField, IntegerField
     * 		Excel-Date 	<-> DateField
     * 		Excel-String <-> All other fields, including ListField
     * Special case: a null field will be written as an empty string
     */
    private void writeField(String fieldName, IField field) throws IOException{
    	if(field == null){
    		return;
    	}
    	
    	if (field instanceof DoubleField) {
    		jsonGenerator.writeNumberField(fieldName, (double) field.getValue());
    	} else if (field instanceof IntegerField) {
    		jsonGenerator.writeNumberField(fieldName, (double) (int) field.getValue());
    	} else if(field instanceof DateField){
    		jsonGenerator.writeStringField(fieldName, field.getValue().toString());
    	} else if (field instanceof ListField) {
    		List<Span> allFields = (List<Span>) field.getValue();
    		jsonGenerator.writeFieldName(fieldName);
    		jsonGenerator.writeStartArray();
    		for (int i = 0; i < allFields.size(); ++i) {
    			Span currentSpan = allFields.get(i);
    			jsonGenerator.writeStartObject();
    			jsonGenerator.writeStringField("attributeName", currentSpan.getAttributeName());
    			jsonGenerator.writeNumberField("start", currentSpan.getStart());
    			jsonGenerator.writeNumberField("end", currentSpan.getEnd());
    			jsonGenerator.writeStringField("key", currentSpan.getKey());
    			jsonGenerator.writeStringField("value", currentSpan.getValue());
    			jsonGenerator.writeNumberField("tokenOffset", currentSpan.getTokenOffset());
    			jsonGenerator.writeEndObject();
    		}
    		jsonGenerator.writeEndArray();
    	} else{
    		jsonGenerator.writeStringField(fieldName, field.getValue().toString());
    	}
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
    	inputOperator.close();
        try {
        	jsonGenerator.writeEndArray();
        	jsonGenerator.close();
        	cursor = CLOSED; 
		} catch (IOException e) {
		    throw new DataflowException(e);
		}
    }

    
    /**
     * Collects ALL the tuples to an in-memory list
     * also add all tuples into the json file by calling this.getNextTuple
     * 
     * @return a list of tuples
     * @throws TexeraException
     */
    public List<Tuple> collectAllTuples() throws TexeraException {
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(tuple);
        }
        return results;
    }
    
    public Path getFilePath() {
        return jsonIndexDirectory.resolve(fileName);
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
