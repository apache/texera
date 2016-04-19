/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.StringField;

/**
 * @author sandeepreddy602
 *
 */
public class KeywordPredicate implements IPredicate{

	private final String fieldName;
    private final String query;
    private ArrayList<String> tokens;
    private Analyzer analyzer;

    public KeywordPredicate(String query, String fieldName, Analyzer analyzer ) throws DataFlowException{
    	try {
    		this.query = query;
            this.fieldName = fieldName;
            this.tokens = queryTokenizer(analyzer, this.query);
            this.analyzer = analyzer;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
    
    @Override
    public boolean satisfy(ITuple tuple) {
        if(tuple == null){
            return false;
        }
        IField field = tuple.getField(fieldName);
        if(field instanceof StringField){
            String fieldValue = ((StringField) field).getValue();
            if(fieldValue != null){
            	ArrayList<String> valueTokens = queryTokenizer(this.analyzer,fieldValue);
            	for (String token : this.tokens) {
            		if(!valueTokens.contains(token)){
            			return false;
            		}
            	}
            	return true;	
            }
        }
        return false;
    }

    public static ArrayList<String> queryTokenizer(Analyzer analyzer,  String query) {

        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream  = analyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        try{
        	tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String term = charTermAttribute.toString();
                result.add(term);
            }
            tokenStream.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }

        return result;
    }
}
