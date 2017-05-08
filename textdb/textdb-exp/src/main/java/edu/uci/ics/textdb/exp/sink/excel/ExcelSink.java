package edu.uci.ics.textdb.exp.sink.excel;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

/**
 * ExcelSink is a sink that can write a list of tuples into an excel file
 * The path of saved files is "textdb/textdb/textdb-perftest/src/main/resources/index/excel/"
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
    
    private String excelIndexDirectory = Paths.get(Utils.getTextdbHomePath(), "index", "excel").toString();
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
    public void open() throws TextDBException{
        if (cursor != CLOSED) {
            return;
        }
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema(inputSchema.getAttributes().stream()
                .filter(attr -> ! attr.getAttributeName().equalsIgnoreCase(SchemaConstants._ID))
                .filter(attr -> ! attr.getAttributeName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
                .filter(attr -> ! attr.getAttributeType().equals(AttributeType.LIST))
                .toArray(Attribute[]::new));
        
        wb = new XSSFWorkbook();
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        fileName = df.format(new Date()) + ".xlsx";
    	try {
    	    if (Files.notExists(Paths.get(excelIndexDirectory))) {
    	        Files.createDirectories(Paths.get(excelIndexDirectory));
    	    }
			fileOut = new FileOutputStream(Paths.get(excelIndexDirectory, fileName).toString());
		} catch (IOException e) {
			throw new DataFlowException(e);
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
    public void processTuples() throws TextDBException {
        return;
    }
    
    /**
     * getNextTuple() will return the cleaned tuple from previous inputOperator.
     * At the same time, it creates a new row in excel and writes the cleaned tuple to that row.
     * List field is kept as one single string.
     */
    @Override
    public Tuple getNextTuple() throws TextDBException {
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
        
        Tuple resultTuple = Utils.removeFields(inputTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
    	Row row = sheet.createRow(cursor-predicate.getOffset());
    	
    	for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {    	    
    	    writeCell(row.createCell(i), resultTuple.getField(outputSchema.getAttributeNames().get(i)));
    	}
    	
        return resultTuple;
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
    	inputOperator.close();
        try {
            wb.write(fileOut);
			fileOut.close();
            cursor = CLOSED; 
		} catch (IOException e) {
		    throw new DataFlowException(e);
		}
    }

    
    /**
     * Collects ALL the tuples to an in-memory list
     * also add all tuples into the excel file by calling this.getNextTuple
     * 
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
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
    		cell.setCellValue((Date) field.getValue());
    	} else{
    		cell.setCellValue(field.getValue().toString());
    	}
    }
    
    public String getFilePath() {
        return Paths.get(excelIndexDirectory, fileName).toString();
    }
    
}
