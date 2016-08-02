package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Scanner;

import org.json.*;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils; 

public class TweetsReader {
	
	 
	public static int cursor = 0;
	private static Scanner scanner;
	
	/*
	 * Be sure to call open() before calling getNextTuple() !
	 * So we have a common relative path "./data-files/FILE_NAME".
	 * Data files under that folder will also be ignored by git.
	 */
	public static void open(String tweetsFilePath) throws FileNotFoundException {
		scanner = new Scanner(new File(tweetsFilePath));
	}
	
	/*
	 * Be sure to call close() in the end !
	 */
	public static void close() {
		if (scanner != null) {
			scanner.close();
		}
	}
	
	/*
	 * Get next Tweets Tuple.
	 * Get one tuple at a time instead of putting them in a list because
	 * if the list gets too big, we will get OutOfMemoryError.
	 */
	public static ITuple getNextTuple() {
		String record = "";
		if (scanner == null) {
			return null;
		}
		if (! scanner.hasNextLine()) {
			return null;
		}
		
		try {
			  record = scanner.nextLine();
			
			 return recordToTuple(record);
		} catch (JSONException|ParseException e) {
			System.out.println("record not added");
			return getNextTuple();
		}
	}
	
	private static ITuple recordToTuple(String record) throws JSONException, ParseException {
		JSONObject json = new JSONObject(record);
		ArrayList<IField> fieldList = new ArrayList<IField>();
		for (Attribute attr: TweetsConstants.ATTRIBUTES_TWEETS) {
		 
			if(! attr.getFieldName().equals(TweetsConstants.HASHTAGS)) {
					fieldList.add(Utils.getField(attr.getFieldType(), json.get(attr.getFieldName()).toString()) );
				
				 
			}
			else {
				
				StringBuilder hashtagsStr = new StringBuilder();
				JSONArray hashtags = json.getJSONObject(TweetsConstants.ENTITIES).getJSONArray(TweetsConstants.HASHTAGS);
				if(hashtags.length() ==0){
					fieldList.add(Utils.getField(attr.getFieldType(), "" ));
					continue;
				}
				for(int i = 0; i < hashtags.length()-1; i++)	{
						 String hashtag = hashtags.getJSONObject(i).get(TweetsConstants.TEXT).toString(); 
						 hashtagsStr.append(hashtag+" ");
				}
				hashtagsStr.append(hashtags.getJSONObject(hashtags.length()-1).get(TweetsConstants.TEXT).toString());
				fieldList.add(Utils.getField(attr.getFieldType() , hashtagsStr.toString())); 
			}
			
		}
		IField[] fieldArray = new IField[fieldList.size()];
		ITuple tuple = new DataTuple(TweetsConstants.SCHEMA_TWEETS, fieldList.toArray(fieldArray));
		return tuple;
	}

}
