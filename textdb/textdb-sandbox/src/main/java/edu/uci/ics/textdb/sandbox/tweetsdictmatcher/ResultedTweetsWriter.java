package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class ResultedTweetsWriter {

	public static void writeResultedTweetsToIndex(String getResultFrom, String writeTo,KeywordMatchingType srcOpType, boolean override, String... query) 
			throws StorageException, Exception  {
	
	ITuple tuple = null;
	IDataStore dataStore = new DataStore(writeTo, TweetsConstants.SCHEMA_TWEETS) ;
	Analyzer analyzer = new StandardAnalyzer();
	DataWriter dataWriter = new DataWriter(dataStore, analyzer);
	if(override){ 
		dataWriter.clearData();
	}
	dataWriter.open();
	TweetsDictMatcher.open(getResultFrom, srcOpType , query);
	
    while ((tuple = TweetsDictMatcher.getNextTuple()) != null) {
    	dataWriter.writeTuple(removeSpan(tuple));	
    
    }
    TweetsDictMatcher.close();
	dataWriter.close();
	 
	 
}
	
	
	public static ITuple removeSpan(ITuple tuple){
	ITuple newtuple = null;
	
	List<IField> fields = tuple.getFields();
	IField[] newfields = new IField[fields.size()-1];
	newfields[0] = fields.get(0);
	newfields[1] = fields.get(1);
	newfields[2] = fields.get(2);
	newtuple = new DataTuple(TweetsConstants.SCHEMA_TWEETS, newfields);
	
	return newtuple;
	
}
	
}
