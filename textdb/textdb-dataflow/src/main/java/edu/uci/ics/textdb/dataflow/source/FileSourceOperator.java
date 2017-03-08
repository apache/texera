package edu.uci.ics.textdb.dataflow.source;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;

/**
 * FileSourceOperator treats files on disk as a source. FileSourceOperator reads
 * a file line by line. A user needs to provide a custom function to convert a
 * string to tuple.
 * 
 * @author zuozhi
 */
public class FileSourceOperator implements ISourceOperator {

    @FunctionalInterface
    public static interface ToTuple {
        Tuple convertToTuple(String str) throws Exception;
    }

    private File file;
    private Scanner scanner;
    private ToTuple toTupleFunc;
    private Schema outputSchema;

    public FileSourceOperator(String filePath, ToTuple toTupleFunc, Schema schema) {
        this.file = new File(filePath);
        this.toTupleFunc = toTupleFunc;
        this.outputSchema = schema;
    }

    @Override
    public void open() throws TextDBException {
        try {
            this.scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            throw new TextDBException("Failed to open FileSourceOperator", e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (scanner.hasNextLine()) {
            try {
                return this.toTupleFunc.convertToTuple(scanner.nextLine());
            } catch (Exception e) {
                e.printStackTrace(System.err);
                return getNextTuple();
            }
        }
        return null;
    }

    @Override
    public void close() throws TextDBException {
        if (this.scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
