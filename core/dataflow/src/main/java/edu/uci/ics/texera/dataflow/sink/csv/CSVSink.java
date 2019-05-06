package edu.uci.ics.texera.dataflow.sink.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import au.com.bytecode.opencsv.CSVWriter;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;

public class CSVSink implements ISink {

    private CSVSinkPredicate predicate;
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
        
    private CSVWriter csvWriter;

    private int cursor = CLOSED;
    
    private Path csvIndexDirectory = Utils.getDefaultIndexDirectory().resolve("csv");
    private String fileName;

    
    public CSVSink(CSVSinkPredicate predicate) {
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
                .filter(attr -> ! attr.getType().equals(AttributeType.LIST))
                .toArray(Attribute[]::new));
        
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        fileName = df.format(new Date()) + ".csv";
        File file = new File(csvIndexDirectory.resolve(fileName).toString()); 

    	try {
    	    if (Files.notExists(csvIndexDirectory)) {
    	        Files.createDirectories(csvIndexDirectory);
    	    }
    	    csvWriter = new CSVWriter(new FileWriter(file));
    	} catch (IOException e) {
			throw new DataflowException(e);
		}
    	
    	// write csv headers
    	List<String> attributeNames = outputSchema.getAttributeNames();
    	csvWriter.writeNext(attributeNames.stream().toArray(String[]::new));
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        return;
    }
    
    /**
     * getNextTuple() will return the cleaned tuple from previous inputOperator.
     * At the same time, it creates a new row in csv and writes the cleaned tuple to that row.
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
        
        // each row in csv is a String[]
        String[] newCSVRow = new String[outputSchema.getAttributeNames().size()];
        for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {
        	newCSVRow[i] = resultTuple.getField(outputSchema.getAttributeNames().get(i)).getValue().toString();
        }
        
        csvWriter.writeNext(newCSVRow);
    	
        return resultTuple;
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
    	inputOperator.close();
        try {
			csvWriter.close();
            cursor = CLOSED; 
		} catch (IOException e) {
		    throw new DataflowException(e);
		}
    }

    
    /**
     * Collects ALL the tuples to an in-memory list
     * also add all tuples into the csv file by calling this.getNextTuple
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
        return csvIndexDirectory.resolve(fileName);
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }

}
