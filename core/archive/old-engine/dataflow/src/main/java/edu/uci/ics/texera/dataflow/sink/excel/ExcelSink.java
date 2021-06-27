package edu.uci.ics.texera.dataflow.sink.excel;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;

/**
 * ExcelSink is a sink that can write a list of tuples into an excel file
 * The path of saved files is "texera/texera/perftest/src/main/resources/index/excel/"
 * @author Jinggang Diao
 *
 */
public class ExcelSink implements ISink {
    
    private ExcelSinkPredicate predicate;
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
        
    private Workbook wb;
    private FileOutputStream fileOut;
    private Sheet sheet;
    private int cursor = CLOSED;
    
    private Path excelIndexDirectory = Utils.getDefaultIndexDirectory().resolve("excel");
    private String fileName;

    
    public ExcelSink(ExcelSinkPredicate predicate) {
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
        
        wb = new XSSFWorkbook();
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        fileName = df.format(new Date()) + ".xlsx";
    	try {
    	    if (Files.notExists(excelIndexDirectory)) {
    	        Files.createDirectories(excelIndexDirectory);
    	    }
			fileOut = new FileOutputStream(excelIndexDirectory.resolve(fileName).toString());
		} catch (IOException e) {
			throw new DataflowException(e);
		}
    	sheet = wb.createSheet("new sheet");
    	Row row = sheet.createRow(0);
    	List<String> attributeNames = outputSchema.getAttributeNames();
    	for(int i = 0; i < attributeNames.size(); i++) {
    		String attributeName = attributeNames.get(i);
        	row.createCell(i).setCellValue(attributeName);
    	}
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        return;
    }
    
    /**
     * getNextTuple() will return the cleaned tuple from previous inputOperator.
     * At the same time, it creates a new row in excel and writes the cleaned tuple to that row.
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
        	Row row = sheet.createRow(cursor-predicate.getOffset());
        	
        	for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {    	    
        	    writeCell(row.createCell(i), resultTuple.getField(outputSchema.getAttributeNames().get(i)));
        	}
    	
        return resultTuple;
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
    	inputOperator.close();
        try {
            wb.write(fileOut);
			fileOut.close();
            cursor = CLOSED; 
		} catch (IOException e) {
		    throw new DataflowException(e);
		}
    }

    
    /**
     * Collects ALL the tuples to an in-memory list
     * also add all tuples into the excel file by calling this.getNextTuple
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
    
    /*
     * Write a field into a cell base on the field type.
     * 		Excel-Double <-> DoubleField, IntegerField
     * 		Excel-Date 	<-> DateField
     * 		Excel-String <-> All other fields, including ListField
     * Special case: a null field will be written as an empty string
     */
    private static void writeCell(Cell cell, IField field){
    	if(field == null){
    		cell.setCellValue("");
    		return;
    	}
    	if (field instanceof DoubleField) {
    	    cell.setCellValue((double) field.getValue());
    	} else if (field instanceof IntegerField) {
    	    cell.setCellValue((double) (int) field.getValue());
    	} else if(field instanceof DateField){
    		cell.setCellValue(field.getValue().toString());
    	} else{
    		cell.setCellValue(field.getValue().toString());
    	}
    }
    
    public Path getFilePath() {
        return excelIndexDirectory.resolve(fileName);
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
    
}
