package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

 
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class test {
	public static void main(String[] args){
 
		dictionaryMatcherTest("../resultIndex",  DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, 
				    "amazing");
 
	}
	
	public static void reusltWriterTest(){
		try {
		ResultedTweetsWriter.writeResultedTweetsToIndex(DataConstants.INDEX_DIR, "../resultIndex",
				DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, true, "zika" );
	} catch (Exception e) {
	 
		e.printStackTrace();
	}
	}
	
	public static void dictionaryMatcherTest(String path, DataConstants.KeywordMatchingType op, String... query){
		ITuple tuple = null;
		int count = 0;
		try {
			TweetsDictMatcher.open(path, op, query);
			 while ((tuple = TweetsDictMatcher.getNextTuple()) != null) {
		    	 System.out.println(tuple);	
		    	 count++;
		    }
		    TweetsDictMatcher.close();
		} catch (Exception e) {
		 
			e.printStackTrace();
		}
		System.out.println(count);
	}
	 
	
	public static void writerTest() {
		IDataStore dataStore = new DataStore(DataConstants.INDEX_DIR, TweetsConstants.SCHEMA_TWEETS) ;
		Analyzer analyzer = new StandardAnalyzer();
		try {
			TweetsIndexWriter.writeTweetsToIndex("C:\\Users\\Hailey\\Desktop\\sampleTweets.json", dataStore, analyzer, true);
		} catch (FileNotFoundException | StorageException e) {
		 
			e.printStackTrace();
		}
		
//		System.out.println("-----Finished-------");
	}
	
	public static void readerTest(){
		try {
			TweetsReader.open("C:\\Users\\Hailey\\Desktop\\sampleTweets.json");
		} catch (FileNotFoundException e) {
	 
			e.printStackTrace();
		}	
		ITuple tuple = null;
		while((tuple= TweetsReader.getNextTuple()) != null ){
				 
//			System.out.println(tuple.toString());
		}	
		TweetsReader.close();
	}
}
