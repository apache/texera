package edu.uci.ics.textdb.dataflow.utils;



import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.SchemaConstants;


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
    
    public static boolean checkResults(List<ITuple> results, String queryString, Analyzer queryAnalyzer, String searchField) throws ParseException {
        
    	
    	boolean contains = false;
        QueryParser parser = null;
        parser = new QueryParser(searchField, queryAnalyzer);
        Query query = parser.parse(queryString);
        String searchString = query.toString(searchField);
        System.out.println(query.toString(searchField));
        String[] listOfQueryWords = searchString.split(" ");
        
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
    
    public static boolean checkSpan(List<ITuple> sampleTuples, ITuple actualTuple, List<Attribute> schema) {
        boolean contains = false;
        int schemaSize = schema.size();
        for (ITuple sampleTuple : sampleTuples) {
            
            for (int i = 0; i < schemaSize; i++) {
            	contains = true;
            	String field =  (String) actualTuple.getField(SchemaConstants.SPAN_FIELD_NAME).getValue();
            	String fieldValue = (String) sampleTuple.getField(field).getValue();
            	String actualValue =  (String) actualTuple.getField(SchemaConstants.SPAN_KEY).getValue();
            	int actualStart = (int) actualTuple.getField(SchemaConstants.SPAN_BEGIN).getValue();
            	int actualEnd = (int) actualTuple.getField(SchemaConstants.SPAN_END).getValue();
            	
                if (actualStart == fieldValue.indexOf(actualValue, actualStart)) {
                	
                	if(actualEnd == (actualStart + actualValue.length() - 1))
                    {
                		contains = true;
                		return contains;
                    }
                	else contains = false;
                }
                else contains = false;
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

    public static boolean equalTo(IField field1, IField field2) {
        return field1.getValue().equals(field2.getValue());
    }
}
