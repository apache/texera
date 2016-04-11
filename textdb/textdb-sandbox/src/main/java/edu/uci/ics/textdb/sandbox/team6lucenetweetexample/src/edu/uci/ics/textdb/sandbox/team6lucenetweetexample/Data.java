package edu.uci.ics.textdb.sandbox.team6lucenetweetexample;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Data {
    
    private static List<Tweet> tweets;
    
    public static boolean loadTweets(File file) throws FileNotFoundException{
        Scanner s = new Scanner(file);
        tweets = new ArrayList<Tweet>();
        while(s.hasNextLine()){
            String line = s.nextLine();
            String[] linea = line.split(",");
            String id = linea[1];
            String date = linea[2];
            String user = linea[4];
            String text = linea[5];
            tweets.add(new Tweet(id, date, user, text));
        }
        s.close();
        return true;
    }

    public static List<Tweet> getTweets() {
//        return (Tweet[])tweets.toArray();
    	return tweets;
    }

    public static Tweet getTweet(String id) {
        for (Tweet tweet : tweets) {
            if (id.equals(tweet.getId())) {
                return tweet;
            }
        }
        return null;
    }
}
