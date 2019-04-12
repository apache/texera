
package edu.uci.ics.texera.perftest.twitter;

import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;

/**
 * A helper class to query data from the main Asterix database with more than 1 billion of tweets based on a set of keywords,
 *  and then writes the data to a table.
 * 
 * @author Zuozhi Wang
 */
public class AsterixTwitterIngest {
    
    public static void main(String[] args) {
        ingestKeywords("16_twitter_immigration_policy_study_2", 
                "immigration daca muslimban nobannowall buildthewall immigrants heretostay refugeeswelcome travelban refugees defenddaca nomuslimban immigrant deportation",
                null, null, null);
    }

    public static void ingestKeywords(String tableName, String keywords, String startDate, String endDate, Integer limit) {
        
        AsterixSourcePredicate asterixSourcePredicate = new AsterixSourcePredicate(
                "twitterJson",
                "actinium.ics.uci.edu",
                19002,
                "twitter",
                "ds_tweet",
                "text",
                keywords,
                startDate,
                endDate,
                limit);

        AsterixSource asterixSource = asterixSourcePredicate.newOperator();

        TwitterSample.createTwitterTable(tableName, asterixSource);
    }

}
