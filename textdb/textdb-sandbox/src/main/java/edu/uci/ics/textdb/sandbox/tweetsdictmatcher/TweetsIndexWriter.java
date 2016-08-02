package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

import java.io.FileNotFoundException;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.sandbox.tweetsdictmatcher.TweetsReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class TweetsIndexWriter {
	
	/*
	 * Write tweet records from "tweetsFilePath"
	 * to index in "indexPath".
	 * 
	 * If override is true, override the "indexPath" with new indices.
	 * else append the new indices to the "indexPath"
	 */
	public static void writeTweetsToIndex(
			String filePath, IDataStore dataStore, Analyzer luceneAnalyzer, boolean override) 
			throws FileNotFoundException, StorageException {
		writeTweetsToIndex(filePath, dataStore, luceneAnalyzer, Integer.MAX_VALUE, override);
	}

	/*
	 * Write a maximum of "maxDocNumber" records
	 * from "tweetsFilePath"
	 * to index in "indexPath".
	 */
	public static void writeTweetsToIndex(
			String filePath, IDataStore dataStore, Analyzer luceneAnalyzer, int maxDocNumber, boolean override) 
			throws FileNotFoundException, StorageException {

		DataWriter dataWriter = new DataWriter(dataStore, luceneAnalyzer);
		
		if(override){
			dataWriter.clearData();
		}
		dataWriter.open();
		
		TweetsReader.open(filePath);
		
		int counter = 0;
		ITuple tuple = null;
		while ((tuple = TweetsReader.getNextTuple()) != null 
				&& counter < maxDocNumber) {
			try {
				dataWriter.writeTuple(tuple);
				counter++;
			} catch (Exception e) {		
					e.printStackTrace();
			}
		}
		
		TweetsReader.close();
		dataWriter.close();

	}
	
}
