package edu.uci.ics.textdb.dataflow.source;

import java.io.File;
import java.util.Scanner;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

/**
 * FileSourceOperator treats files on disk as a source.
 * FileSourceOperator reads a file line by line.
 * A user needs to provide a custom function to convert a string to tuple. 
 * @author zuozhi
 */
public class FileSourceOperator implements ISourceOperator {
	
	@FunctionalInterface
	public static interface ToTuple {
		ITuple convertToTuple(String str) throws Exception;
	}
	
	private File file;
	private Scanner scanner;
	private ToTuple toTupleFunc;

	public FileSourceOperator(String filePath, ToTuple toTupleFunc) {
		this.file = new File(filePath);
		this.toTupleFunc = toTupleFunc;
	}

	@Override
	public void open() throws Exception {
		this.scanner = new Scanner(file);
	}

	@Override
	public ITuple getNextTuple() throws Exception {
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
	public void close() throws Exception {
		if (this.scanner != null) {
			this.scanner.close();
		}
	}
	
}
