package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 *  @author prakul
 *  @author Akshay
 *  @author Zuozhi Wang
 *
 */
public class KeywordMatcher implements IOperator {
    private final KeywordPredicate predicate;
	private String query;
	
    private IOperator inputOperator;
    private Schema inputSchema;
    private Schema outputSchema;
    
    private boolean spanAdded = true;

    /**
     * Constructs a KeywordMatcher that use dataStore as its input source
     * @param predicate,
     * @param dataStore
     */
	public KeywordMatcher(IPredicate predicate, IDataStore dataStore) {
		this(predicate);
    
		DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.predicate.getQueryObject(), this.predicate.getQuery(),
				dataStore, this.predicate.getAttributeList(), this.predicate.getLuceneAnalyzer());
        dataReaderPredicate.setIsSpanInformationAdded(true);
        this.inputOperator = new IndexBasedSourceOperator(dataReaderPredicate);
    }
	
	/**
	 * Constructs a KeywordMatcher that uses inputOperator as its input source.
	 * @param predicate
	 * @param inputOperator
	 */
	public KeywordMatcher(IPredicate predicate, IOperator inputOperator) {
		this(predicate);
        this.inputOperator = inputOperator;
	}	

	/**
	 * Constructs a KeywordMatcher with no input source. 
	 * To specify its input operator, call setInputOperator().
	 * @param predicate
	 */
	public KeywordMatcher(IPredicate predicate) {
        this.predicate = (KeywordPredicate) predicate;
        this.query = this.predicate.getQuery();
    }
	


    @Override
    public void open() throws DataFlowException {
    	if (inputOperator == null) {
    		throw new DataFlowException(ErrorMessages.NO_INPUT_OPERATOR);
    	}
        try {
            inputOperator.open();
            inputSchema = inputOperator.getOutputSchema();
            
            if (spanAdded && (inputSchema.getIndex(SchemaConstants.SPAN_LIST) == null)) {
				outputSchema = Utils.createSpanSchema(inputSchema);
            } else {
                outputSchema = inputSchema;
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    
    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan based search or an index based search depending
     *        on the sourceOperator provided while initializing KeywordPredicate.
     *        It scans documents returned by sourceOperator for provided keywords/phrase (Depending on provided
     *        KeywordOperatorType).
     *        
     *        See DataConstants.KeywordMatchingType for 3 types of keyword matching.
     */
    @Override
    public ITuple getNextTuple() throws DataFlowException {
    	if (inputOperator == null) {
    		throw new DataFlowException(ErrorMessages.NO_INPUT_OPERATOR);
    	}
        try {
        	ITuple result = null;
        	do {
                ITuple sourceTuple = inputOperator.getNextTuple();
                if(sourceTuple == null) {
                    return null;
                }
                
                if (spanAdded && (inputSchema.getIndex(SchemaConstants.SPAN_LIST) == null)) {
                	sourceTuple = Utils.getSpanTuple(sourceTuple.getFields(), new ArrayList<Span>(), outputSchema);
    				outputSchema = Utils.createSpanSchema(inputSchema);
                }
                
                if (predicate.getOperatorType() == DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED) {
                	result = processConjunction(sourceTuple);
                }
                if (predicate.getOperatorType() == DataConstants.KeywordMatchingType.PHRASE_INDEXBASED) {
                	result = processPhrase(sourceTuple);
                }
                if (predicate.getOperatorType() == DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED) {
                	result = processSubstring(sourceTuple);
                }
                
        	} while (result == null);

            return result;
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }

    
    
    private ITuple processConjunction(ITuple currentTuple) throws DataFlowException {
    	if (inputSchema == null) {
    		throw new RuntimeException("input schema is null");
    	}
    	if (! inputSchema.hasField(SchemaConstants.PAYLOAD)) {
    		throw new RuntimeException("TODO: create term vector if it doesn't exist");
    	}
    	
    	List<Span> termVec = (List<Span>) currentTuple.getField(SchemaConstants.PAYLOAD).getValue(); 
    	List<Span> relevantSpans = filterRelevantSpans(termVec);
    	List<Span> matchResults = new ArrayList<>();
    	
    	for (Attribute attribute : predicate.getAttributeList()) {
    		String fieldName = attribute.getFieldName();
    		FieldType fieldType = attribute.getFieldType();
			String fieldValue = currentTuple.getField(fieldName).getValue().toString();
			
    		// types other than TEXT and STRING: throw Exception for now
			if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
	    		throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
			}
    		
			// for STRING type, the query should match the fieldValue completely
    		if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(query)) {
                    Span span = new Span(fieldName, 0, query.length(), query, fieldValue);
                    matchResults.add(span);
                }
    		}
    		
    		// for TEXT type, every token in the query should be present in span list for this field
    		if (fieldType == FieldType.TEXT) {
        		List<Span> fieldSpanList = relevantSpans.stream()
        				.filter(span -> span.getFieldName().equals(fieldName))
        				.collect(Collectors.toList());
        		
        		if (isAllQueryTokensPresent(fieldSpanList, predicate.getQueryTokenSet())) {
        			matchResults.addAll(fieldSpanList);
        		}
    		}
    		
    	}

    	if (matchResults.isEmpty()) {
    		return null;
    	}
    	
    	if (spanAdded) {
        	List<Span> spanList = (List<Span>) currentTuple.getField(SchemaConstants.SPAN_LIST).getValue(); 
        	spanList.addAll(matchResults);
    	}
    	
    	return currentTuple;
    }
    
    
    private ITuple processPhrase(ITuple currentTuple) throws DataFlowException {
    	if (! inputSchema.hasField(SchemaConstants.PAYLOAD)) {
    		throw new RuntimeException("TODO: create term vector if it doesn't exist");
    	}
    	
    	List<Span> termVec = (List<Span>) currentTuple.getField(SchemaConstants.PAYLOAD).getValue(); 
    	List<Span> relevantSpans = filterRelevantSpans(termVec);
    	List<Span> matchResults = new ArrayList<>();
    	
    	for (Attribute attribute : predicate.getAttributeList()) {
    		String fieldName = attribute.getFieldName();
    		FieldType fieldType = attribute.getFieldType();
    		String fieldValue = currentTuple.getField(fieldName).getValue().toString();
    		
    		// types other than TEXT and STRING: throw Exception for now
			if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
	    		throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
			}
    		
			// for STRING type, the query should match the fieldValue completely
    		if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(query)) {
                	matchResults.add(new Span(fieldName, 0, query.length(), query, fieldValue));
                }
    		}
    		
    		// for TEXT type, spans need to be reconstructed according to the phrase query
    		if (fieldType == FieldType.TEXT) {
        		List<Span> fieldSpanList = relevantSpans.stream()
        				.filter(span -> span.getFieldName().equals(fieldName))
        				.collect(Collectors.toList());
        		
        		if (! isAllQueryTokensPresent(fieldSpanList, predicate.getQueryTokenSet())) {
        			// move on to next field if not all query tokens are present in the spans
        			continue;
        		}
        		
            	// Sort current field's span list by token offset for later use
            	Collections.sort(fieldSpanList, (span1, span2) -> span1.getTokenOffset()-span2.getTokenOffset());
            	
            	List<String> queryTokenList = predicate.getQueryTokenList();
            	List<Integer> queryTokenOffset = new ArrayList<>();
            	List<String> queryTokensWithStopwords = predicate.getQueryTokensWithStopwords();
            	
                for(int i = 0; i < queryTokensWithStopwords.size(); i++) {
                    if (queryTokenList.contains(queryTokensWithStopwords.get(i))) {
                        queryTokenOffset.add(i);
                    }
                }

                int iter = 0; // maintains position of term being checked in spanForThisField list
                while(iter < fieldSpanList.size()) {
                    if (iter > fieldSpanList.size()-queryTokenList.size()) {
                    	break;
                    }
                    
                    // Verify if span in the spanForThisField correspond to our phrase query, ie relative position offsets should be similar
                    // and the value should be same.
                    boolean isMismatchInSpan=false;// flag to check if a mismatch in spans occurs
                    
                     // To check all the terms in query are verified
                    for(int i=0; i < queryTokenList.size()-1; i++) {
                        Span first = fieldSpanList.get(iter+i);
                        Span second = fieldSpanList.get(iter +i+ 1);
                        if (!(second.getTokenOffset() - first.getTokenOffset() == queryTokenOffset.get(i+1) - queryTokenOffset.get(i) &&
                                first.getValue().equalsIgnoreCase(queryTokenList.get(i)) && second.getValue().equalsIgnoreCase(queryTokenList.get(i+1)))) {
                            iter++;
                            isMismatchInSpan=true;
                            break;
                        }
                    }
                    
                    if(isMismatchInSpan) {
                        continue;
                    }

                    int combinedSpanStartIndex = fieldSpanList.get(iter).getStart();
                    int combinedSpanEndIndex = fieldSpanList.get(iter+queryTokenList.size()-1).getEnd();

                    Span combinedSpan = new Span(fieldName, combinedSpanStartIndex, combinedSpanEndIndex, query, fieldValue.substring(combinedSpanStartIndex, combinedSpanEndIndex));
                    matchResults.add(combinedSpan);
                    iter = iter + queryTokenList.size();
                }
    		}
    	}
    	
    	if (matchResults.isEmpty()) {
    		return null;
    	}

    	if (spanAdded) {
        	List<Span> spanList = (List<Span>) currentTuple.getField(SchemaConstants.SPAN_LIST).getValue(); 
        	spanList.addAll(matchResults);
    	}

    	return currentTuple;
    }
    
    
    private ITuple processSubstring(ITuple currentTuple) throws DataFlowException {
    	List<Span> matchResults = new ArrayList<>();
    	
    	for (Attribute attribute : predicate.getAttributeList()) {
    		String fieldName = attribute.getFieldName();
    		FieldType fieldType = attribute.getFieldType();
    		String fieldValue = currentTuple.getField(fieldName).getValue().toString();
    		
    		
    		// types other than TEXT and STRING: throw Exception for now
			if (fieldType != FieldType.STRING && fieldType != FieldType.TEXT) {
	    		throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
			}
    		
			// for STRING type, the query should match the fieldValue completely
    		if (fieldType == FieldType.STRING) {
                if (fieldValue.equals(query)) {
                	matchResults.add(new Span(fieldName, 0, query.length(), query, fieldValue));
                }
    		}
    		
    		if (fieldType == FieldType.TEXT) {
    			String regex = query.toLowerCase();
    			Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE); 
    			Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
    			while (matcher.find()) {
    				int start = matcher.start();
    				int end = matcher.end();

    				matchResults.add(new Span(fieldName, start, end, query, fieldValue.substring(start, end)));
    			}
    		}
    		
    	}
    	if (matchResults.isEmpty()) {
    		return null;
    	}
    	
    	if (spanAdded) {
        	List<Span> spanList = (List<Span>) currentTuple.getField(SchemaConstants.SPAN_LIST).getValue(); 
        	spanList.addAll(matchResults);
    	}
    	
    	return currentTuple;
    }
    
    
    private List<Span> filterRelevantSpans(List<Span> spanList) {
    	List<Span> filteredList = new ArrayList<>();
    	Iterator<Span> iterator = spanList.iterator();
    	while (iterator.hasNext()) {
    		Span span  = iterator.next();
    		if (predicate.getQueryTokenSet().contains(span.getKey())) {
    			filteredList.add(span);
    		}
    	}
    	return filteredList;
    }
    
    
    private boolean isAllQueryTokensPresent(List<Span> fieldSpanList, Set<String> queryTokenSet) {
		Set<String> fieldSpanKeys = fieldSpanList.stream()
				.map(span -> span.getKey())
				.collect(Collectors.toSet());
		
		return fieldSpanKeys.equals(queryTokenSet);
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
    
    
    public IOperator getInputOperator() {
		return inputOperator;
	}

	public void setInputOperator(IOperator inputOperator) {
		this.inputOperator = inputOperator;
	}
    
    public boolean isTermVecAdded() {
		return spanAdded;
	}

	public void setTermVecAdded(boolean termVecAdded) {
		this.spanAdded = termVecAdded;
	}
	
	public Schema getOutputSchema() {
		return outputSchema;
	}

}