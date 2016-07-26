package edu.uci.ics.textdb.dataflow.utils;



import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.ParseException;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;


/**
 * @author sandeepreddy602
 * @author zuozhi
 * @author rajeshyarlagadda
 */
public class TestUtils {

    public static boolean contains(List<ITuple> sampleTuples, ITuple actualTuple, List<Attribute> schema) {
        boolean contains = false;
        int schemaSize = schema.size();
        for (ITuple sampleTuple : sampleTuples) {
            contains = true;
            for (int i = 0; i < schemaSize; i++) {
                if (!sampleTuple.getField(i).equals(actualTuple.getField(i))) {
                    contains = false;
                }
            }
            if (contains) {
                return contains;
            }
        }
        return contains;
    }
    
    public static boolean containsAllResults(List<ITuple> expectedResults, List<ITuple> exactResults) {
        if(expectedResults.size() != exactResults.size())
        	return false;
        if(!(expectedResults.containsAll(exactResults)) || !(exactResults.containsAll(expectedResults)))
        	return false;
        
        return true;
    }
    
    public static boolean containsAllResults(ArrayList<String> expectedStrings, ArrayList<String> exactStrings) {
        if(expectedStrings.size() != exactStrings.size())
        	return false;
        if(!(expectedStrings.containsAll(exactStrings)) || !(exactStrings.containsAll(expectedStrings)))
        	return false;
        
        return true;
    }
    
    public static boolean checkResults(List<ITuple> results, String queryString, Analyzer queryAnalyzer, String searchField) throws ParseException {
      
    	boolean contains = false;
        
    	List<String> listOfQueryWords = tokenizeString(queryAnalyzer, queryString);
      
        for (ITuple sampleTuple : results) {
            contains = false;
            String value = (String) sampleTuple.getField(searchField).getValue();
            for (String queryWord : listOfQueryWords) {
            	if(value.toLowerCase().contains(queryWord)) {
            		
            		contains = true;
            		return contains;
            	}
            }
        }
        return contains;
    }
    
   
    public static boolean contains(ArrayList<String> Dictionary, String returnedString) {
        boolean contains = false;

        for (String dictItem : Dictionary) {
            if (dictItem.equals(returnedString)) {
                contains = true;
                return contains;
            }
        }

        return contains;
    }

    public static boolean equalTo(List<ITuple> tuples1, List<ITuple> tuples2) {
        if (tuples1.size() != tuples2.size()) {
            return false;
        }
        for (int i = 0; i < tuples1.size(); ++i) {
            if (!equalTo(tuples1.get(i), tuples2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalTo(ITuple tuple1, ITuple tuple2) {
        List<IField> fields1 = tuple1.getFields();
        List<IField> fields2 = tuple2.getFields();
        if (fields1.size() != fields2.size()) {
            return false;
        }
        for (int i = 0; i < fields1.size(); ++i) {
            if (!equalTo(fields1.get(i), fields2.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public static List<String> tokenizeString(Analyzer analyzer, String string) {
        List<String> result = new ArrayList<String>();
        try {
          TokenStream stream  = analyzer.tokenStream(null, new StringReader(string));
          stream.reset();
          while (stream.incrementToken()) {
            result.add(stream.getAttribute(CharTermAttribute.class).toString());
          }
        } catch (IOException e) {
          // not thrown b/c we're using a string reader...
          throw new RuntimeException(e);
        }
        return result;
      }

    public static boolean equalTo(IField field1, IField field2) {
        return field1.getValue().equals(field2.getValue());
    }
}
