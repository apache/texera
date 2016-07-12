package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

 
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.constants.DataConstants.SourceOperatorType;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.*;
import edu.uci.ics.textdb.storage.DataStore;

public class TweetsDictMatcher {

    private static DictionaryMatcher dictionaryMatcher;
    private static ArrayList<Attribute> attributes = new ArrayList<Attribute>(Arrays.asList(TweetsConstants.ATTRIBUTES_TWEETS));
    private static Analyzer luceneAnalyzer = new StandardAnalyzer();
    
    
    
    public static void open(String indexPath, SourceOperatorType srcOpType, String... query) throws Exception{
    	ArrayList<String> queries = new ArrayList<String>(Arrays.asList(query));
    	IDictionary dictionary = new Dictionary(queries);
    	IDataStore dataStore = new DataStore(indexPath, TweetsConstants.SCHEMA_TWEETS);
    	IPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, luceneAnalyzer, attributes, srcOpType, dataStore);
    	dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
    	dictionaryMatcher.open();
    }
    
 
    
	public static ITuple getNextTuple() throws Exception {
		if(dictionaryMatcher == null){
			return null;
		}
	    ITuple tuple = null;	        
	    tuple = dictionaryMatcher.getNextTuple();
	    return tuple;
	 }
	 
	 public static void close() throws DataFlowException{
		 dictionaryMatcher.close();
	 }
	 
	 
	 

}

