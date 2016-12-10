package edu.uci.ics.textdb.textql.querymanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;


import edu.uci.ics.textdb.plangen.LogicalPlan;

/**
 * Wraps the parser for TextQL and a translator that generates a QueryPlan.
 * 
 * @author Flavio Bayer
 */
public class QueryManager {
	//InputStream used as the source for the underlying parser
	private InputStream input;
	//OutpuStream used as by the underlying parser to print out results of each parsed statement
	private OutputStream output;
	/** parsedStatements is a List of statements that have been parsed from the input
	 each statement is a pair <key,value> for the parameter of the statement
	 each statement has at least the keys "statementType" and "statementName"(identifier)
	*/
	private List<Map<String, Object>> parsedStatements;
	
	
	/**
	 * Set the InputStream used during the parsing 
	 * @param inputStream  the new InputStream
	 */
	public QueryManager(InputStream inputStream){
	}
	
	/**
	 * Create a new InputStream from the file f to be used during the parsing
	 * The content of the file will be used as the whole input
	 * @param f  the File used as input InputStream
	 */
	public QueryManager(File f) throws FileNotFoundException{
	}

	/**
	 * Create a new InputStream from the string s to be used during the parsing 
	 * The content of the string s will be used as the whole input
	 * @param inputStream  the new InputStream
	 */
	public QueryManager(String s) throws IOException{
	}
	
	/**
	 * Create a new InputStream from the string s to be used during the parsing 
	 * The content of the string s will be used as the whole input
	 * @param inputStream  the new InputStream
	 */
	void setOutput(OutputStream outputStream) throws IOException{
	}
	
	/**
	 * Parse the current input and generate internal structures required
	 * to export views using getView or getOutputView.
	 * @throws IOException If an exception occurs while closing Input or Output Streams
	 */
	void parse() throws IOException{
	}
	
	/**
	 * Generates a logical plan for the view defined as output
	 * that is, the view that was used in the "OUTPUT VIEW" statement
	 * @return The generated LogicalPlan for the view
	 */
	LogicalPlan getOutputView(){
		return null;
	}

	/**
	 * Generates a logical plan for the view with specific name.
	 * @param viewName	Name of the view to be selected
	 * @return The generated LogicalPlan for the view
	 */
	LogicalPlan getView(String viewName){		
		return null;
	}
	
}
