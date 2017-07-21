package edu.uci.ics.textdb.exp.twitterfeed;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

import java.util.List;

/**
 * Created by Chang on 7/12/17.
 */
public class TwitterFeedSourcePredicate extends PredicateBase {

    private final int tweetNum;
    private final List<String> keywordQuery;
    private final String locations;
    private final List<String> language;



    public TwitterFeedSourcePredicate(
                                @JsonProperty(value = PropertyNameConstants.TWEET_NUM, required = true)
                                        int tweetNum,
                                @JsonProperty(value = PropertyNameConstants.QUERY_LIST, required=false)
                                        List<String> keywordQuery,
                                @JsonProperty(value = PropertyNameConstants.LOCATION_LIST, required=false)
                                        String locations,
                                @JsonProperty(value = PropertyNameConstants.LANGUAGE_LIST, required=false)
                                        List<String> language){
            this.tweetNum = tweetNum;
            this.keywordQuery = keywordQuery;
            this.locations = locations;
            this.language = language;




    }

    @JsonProperty(PropertyNameConstants.QUERY_LIST)
    public List<String> getQuery() {
        return this.keywordQuery;
    }

    @JsonProperty(PropertyNameConstants.LOCATION_LIST)
    public String getLocations() {
        return this.locations;
    }

    @JsonProperty(PropertyNameConstants.TWEET_NUM)
    public int getTweetNum() {
        return this.tweetNum;
    }

    @JsonProperty(PropertyNameConstants.LANGUAGE_LIST)
    public List<String> getLanguage() {
        return this.language;
    }



    @Override
    public IOperator newOperator() {
       return new TwitterFeedOperator(this);
    }


}
