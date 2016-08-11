package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * Created by chenli on 3/25/16.
 * @author laishuying
 */
public class RegexMatcher implements IOperator {
    private RegexPredicate regexPredicate;
    
    private String regex;
    private List<Attribute> attributeList;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
	private IOperator inputOperator;
    
	private int limit;
	private int cursor;
	private int offset;
        
    // two available regex engines, RegexMatcher will try RE2J first 
	private enum RegexEngine {
		JavaRegex,
		RE2J
	}
	private RegexEngine regexEngine;
	private com.google.re2j.Pattern re2jPattern;
	private java.util.regex.Pattern javaPattern;
	
	
    public RegexMatcher(RegexPredicate predicate) throws DataFlowException{
    	this.cursor = -1;
    	this.offset = 0;
    	this.limit = Integer.MAX_VALUE;
    	this.regexPredicate = predicate;
    	this.regex = regexPredicate.getRegex();
    	this.attributeList = regexPredicate.getAttributeList();  	
    			
		// try Java Regex first
		try {
			this.javaPattern = java.util.regex.Pattern.compile(regex);
			this.regexEngine = RegexEngine.JavaRegex;
		// if Java Regex fails, try RE2J
		} catch (java.util.regex.PatternSyntaxException javaException) {
	    	try {
	    		this.re2jPattern = com.google.re2j.Pattern.compile(regexPredicate.getRegex());
				this.regexEngine = RegexEngine.RE2J;
			// if RE2J also fails, throw exception
	    	} catch (com.google.re2j.PatternSyntaxException re2jException) {
				throw new DataFlowException(javaException.getMessage(), javaException);
	    	}
		}		
    }
    
    
    @Override
    public void open() throws DataFlowException {
        if (this.inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }

        try {
            inputOperator.open();
            
            this.inputSchema = inputOperator.getOutputSchema();
            if (! this.inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                outputSchema = Utils.createSpanSchema(inputSchema);
            } else {
                outputSchema = inputSchema;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
	
	
    @Override
    public ITuple getNextTuple() throws DataFlowException {
		try {
			if (limit == 0 || cursor >= offset + limit - 1){
				return null;
			}
			ITuple sourceTuple;
			ITuple resultTuple = null;
			while ((sourceTuple = inputOperator.getNextTuple()) != null) {
			    if (! inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
			        sourceTuple = Utils.getSpanTuple(sourceTuple.getFields(), new ArrayList<Span>(), outputSchema);
			    }	    
	            resultTuple = computeMatchingResult(sourceTuple);
	            
	            if (resultTuple != null) {
		            cursor++;
	            }
	            if (cursor >= offset){
	            	break;
	            }
			}
			return resultTuple;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }        
    }
    

	/**
	 * This function returns a list of spans in the given tuple that match the
	 * regex For example, given tuple ("george watson", "graduate student", 23,
	 * "(949)888-8888") and regex "g[^\s]*", this function will return
	 * [Span(name, 0, 6, "g[^\s]*", "george
	 * watson"), Span(position, 0, 8, "g[^\s]*", "graduate student")]
	 * 
	 * @param tuple
	 *            document in which search is performed
	 * @return a list of spans describing the occurrence of a matching sequence
	 *         in the document
	 * @throws DataFlowException 
	 */
	public ITuple computeMatchingResult(ITuple sourceTuple) throws DataFlowException {
	    if (sourceTuple == null) {
	        return null;
	    }
	    
        List<Span> matchingResults = new ArrayList<>();
        
        for (Attribute attribute : attributeList) {
            String fieldName = attribute.getFieldName();
            FieldType fieldType = attribute.getFieldType();
            String fieldValue = sourceTuple.getField(fieldName).getValue().toString();
            
            // types other than TEXT and STRING: throw Exception for now
            if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
                        
            switch (regexEngine) {
            case JavaRegex:
                matchingResults.addAll(javaRegexMatch(fieldValue, fieldName));
                break;
            case RE2J:
                matchingResults.addAll(re2jRegexMatch(fieldValue, fieldName));
                break;
            }     
        }

		if (matchingResults.isEmpty()) {
			return null;
		}
			
        List<Span> spanList = (List<Span>) sourceTuple.getField(SchemaConstants.SPAN_LIST).getValue();
        spanList.addAll(matchingResults);
        
		return sourceTuple;
	}
	
	
	private List<Span> javaRegexMatch(String fieldValue, String fieldName) {
	    List<Span> matchingResults = new ArrayList<>();
		java.util.regex.Matcher javaMatcher = this.javaPattern.matcher(fieldValue);
		while (javaMatcher.find()) {
			int start = javaMatcher.start();
			int end = javaMatcher.end();
			matchingResults.add(new Span(fieldName, start, end, 
					this.regexPredicate.getRegex(), fieldValue.substring(start, end)));
		}
		return matchingResults;
	}
	
	private List<Span> re2jRegexMatch(String fieldValue, String fieldName) {
        List<Span> matchingResults = new ArrayList<>();	    
		com.google.re2j.Matcher re2jMatcher = this.re2jPattern.matcher(fieldValue);
		while (re2jMatcher.find()) {
			int start = re2jMatcher.start();
			int end = re2jMatcher.end();
			matchingResults.add(new Span(fieldName, start, end, 
					this.regexPredicate.getRegex(), fieldValue.substring(start, end)));
		}
		return matchingResults;
	}
	
	/**
	 * Use Java's built-in Regex Engine. <br>
	 * RegexMatcher is set to use Java Regex Engine by default. <br>
	 * @throws java.util.regex.PatternSyntaxException
	 */
	public void setRegexEngineToJava() throws java.util.regex.PatternSyntaxException {
		if (this.regexEngine == RegexEngine.JavaRegex) {
			return;
		} else {
			this.javaPattern = java.util.regex.Pattern.compile(this.regex);
			this.regexEngine = RegexEngine.JavaRegex;
		}
	}

	/**
	 * Use RE2J Regex Engine. <br>
	 * RegexMatcher is set to use Java Regex Engine by default. 
	 * Because Java Regex is usually faster than RE2J <br>
	 * @throws java.util.regex.PatternSyntaxException
	 */
	public void setRegexEngineToRE2J() throws java.util.regex.PatternSyntaxException {
		if (this.regexEngine == RegexEngine.RE2J) {
			return;
		} else {
			try {
				this.re2jPattern = com.google.re2j.Pattern.compile(this.regex);
				this.regexEngine = RegexEngine.RE2J;
			} catch (com.google.re2j.PatternSyntaxException e) {
				throw new java.util.regex.PatternSyntaxException(e.getDescription(), e.getPattern(), e.getIndex());
			}
		}
	}
	
	public String getRegexEngineString() {
		return this.regexEngine.toString();
	}
    

    @Override
    public void close() throws DataFlowException {
        try {
            if (inputOperator != null) {
                inputOperator.close();   
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
    
    public String getRegex() {
    	return this.regex;
    }
    
    public IOperator getInputOperator() {
		return inputOperator;
	}

	public void setInputOperator(ISourceOperator inputOperator) {
		this.inputOperator = inputOperator;
	}

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public void setLimit(int limit){
        this.limit = limit;
    }
    
    public int getLimit(){
        return this.limit;
    }
    
    public void setOffset(int offset){
        this.offset = offset;
    }
    
    public int getOffset(){
        return this.offset;
    }
}
