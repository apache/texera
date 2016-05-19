package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import edu.stanford.nlp.patterns.Data;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
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
    private List<String> fieldNameList;
    
    private Schema sourceTupleSchema = null;
    private Schema spanSchema = null;
    
	private Analyzer luceneAnalyzer;
	private IDataStore dataStore;
	private Query luceneQuery;
	private ISourceOperator sourceOperator;
    
    private List<Span> spanList;
    
    // two available regex engines, RegexMatcher will try RE2J first 
	private enum RegexEngine {
		JavaRegex,
		RE2J
	}
	private RegexEngine regexEngine;

    public RegexMatcher(IPredicate predicate) throws DataFlowException{
    	this.regexPredicate = (RegexPredicate) predicate;
    	this.regex = regexPredicate.getRegex();
    	this.fieldNameList = regexPredicate.getFieldNameList();
    	this.luceneAnalyzer = regexPredicate.getLuceneAnalyzer();
    	this.dataStore = regexPredicate.getDataStore();
    	
		try {			
			// try to translate to LuceneQuery using RegexToGramQueryTranslator
			try {
				com.google.re2j.Pattern.compile(regexPredicate.getRegex());
				regexEngine = RegexEngine.RE2J;
				this.luceneQuery = generateLuceneQuery(regex, fieldNameList,
						RegexToGramQueryTranslator.translate(regex).getLuceneQueryString());
			// if RE2J fails, try to use Java Regex
			} catch (com.google.re2j.PatternSyntaxException re2jException) {
				java.util.regex.Pattern.compile(regex);
				regexEngine = RegexEngine.JavaRegex;
				this.luceneQuery = generateLuceneQuery(regex, fieldNameList,
						DataConstants.SCAN_QUERY);
			}
			DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(dataStore, luceneQuery,
					DataConstants.SCAN_QUERY, luceneAnalyzer, regexPredicate.getAttributeList());
			this.sourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
		} catch (ParseException | java.util.regex.PatternSyntaxException e) {
			throw new DataFlowException(e.getMessage(), e);
		}
    }
    
	private Query generateLuceneQuery(String regexStr, List<String> fields, String queryStr) throws ParseException {
		String[] fieldsArray = new String[fields.size()];
		QueryParser parser = new MultiFieldQueryParser(fields.toArray(fieldsArray), luceneAnalyzer);
		return parser.parse(queryStr);
	}
	
    @Override
    public ITuple getNextTuple() throws DataFlowException {
		try {
            ITuple sourceTuple = sourceOperator.getNextTuple();
            if(sourceTuple == null){
                return null;
            }  
            
            this.spanList = computeMatches(sourceTuple);
            
            if (spanList != null && spanList.size() != 0) { // a list of matches found
            	if (sourceTupleSchema == null) {
            		sourceTupleSchema = sourceTuple.getSchema();
            	}
            	if (spanSchema == null) {
            		spanSchema = Utils.createSpanSchema(sourceTupleSchema);
            	}
            	List<IField> fields = sourceTuple.getFields();
            	return constructSpanTuple(fields, this.spanList);
            } else { // no match found
            	return getNextTuple();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }        
    }
    
    private ITuple constructSpanTuple(List<IField> fields, List<Span> spans) {
    	List<IField> fieldListDuplicate = new ArrayList<>(fields);
    	IField spanListField = new ListField<Span>(spans);
    	fieldListDuplicate.add(spanListField);
    	IField[]  fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
    	return new DataTuple(spanSchema, fieldsDuplicate);
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
	 */
	public List<Span> computeMatches(ITuple tuple) {
		List<Span> spanList = new ArrayList<>();
		if (tuple == null) {
			return spanList; // empty array
		}
		for (String fieldName : fieldNameList) {
			IField field = tuple.getField(fieldName);
			String fieldValue = field.getValue().toString();
			if (fieldValue == null) {
				return spanList;
			} else {
				switch (regexEngine) {
				case JavaRegex:
					javaRegexMatch(fieldValue, fieldName, spanList);
					break;
				case RE2J:
					re2jRegexMatch(fieldValue, fieldName, spanList);
					break;
				}
			}
		}
		return spanList;
	}
	
	private void javaRegexMatch(String fieldValue, String fieldName, List<Span> spanList) {
		java.util.regex.Matcher javaMatcher = 
				java.util.regex.Pattern.compile(this.regexPredicate.getRegex())
				.matcher(fieldValue);
		while (javaMatcher.find()) {
			spanList.add(new Span(fieldName, javaMatcher.start(), javaMatcher.end(), this.regexPredicate.getRegex(), fieldValue));
		}
	}
	
	private void re2jRegexMatch(String fieldValue, String fieldName, List<Span> spanList) {
		com.google.re2j.Matcher re2jMatcher = 
		com.google.re2j.Pattern.compile(this.regexPredicate.getRegex())
		.matcher(fieldValue);
		while (re2jMatcher.find()) {
			spanList.add(new Span(fieldName, re2jMatcher.start(), re2jMatcher.end(), this.regexPredicate.getRegex(), fieldValue));
		}
	}


    public Schema getSpanSchema() {
    	return spanSchema;
    }
    
    @Override
    public void open() throws DataFlowException {
        try {
            sourceOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
