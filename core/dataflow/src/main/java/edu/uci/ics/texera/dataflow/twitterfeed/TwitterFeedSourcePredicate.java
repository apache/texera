package edu.uci.ics.texera.dataflow.twitterfeed;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

import java.util.List;
import java.util.Map;

/**
 * Created by Chang on 7/12/17.
 */

/**
 * Twitter streaming API supports "trackTerms" on keyword, location and language.
 * These query parameters along with the number of tweets required
 * can be specified in TwitterFeedSourcePredicate.
 */
public class TwitterFeedSourcePredicate extends PredicateBase {

    private final int tweetNum;
    private final List<String> keywordList;
    private final String locationList;
    private final List<String> languageList;
    private final String customerKey;
    private final String customerSecret;
    private final String token;
    private final String tokenSecret;

    @JsonCreator
    public TwitterFeedSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.TWEET_NUM, required = true)
                    int tweetNum,
            @JsonProperty(value = PropertyNameConstants.TWEET_QUERY_LIST, required = false)
                    List<String> keywordList,
            @JsonProperty(value = PropertyNameConstants.TWEET_LOCATION_LIST, required = false)
                    String locationList,
            @JsonProperty(value = PropertyNameConstants.TWEET_LANGUAGE_LIST, required = false)
                    List<String> languageList,
            @JsonProperty(value = PropertyNameConstants.TWEET_CUSTOMER_KEY, required = false)
                    String customerKey,
            @JsonProperty(value = PropertyNameConstants.TWEET_CUSTOMER_SECRET, required = false)
                    String customerSecret,
            @JsonProperty(value = PropertyNameConstants.TWEET_TOKEN, required = false)
                    String token,
            @JsonProperty(value = PropertyNameConstants.TWEET_TOKEN_SECRET, required = false)
                    String tokenSecret) {

        this.tweetNum = tweetNum;
        this.keywordList = keywordList;
        this.locationList = locationList;
        this.languageList = languageList;
        this.customerKey = customerKey;
        this.customerSecret = customerSecret;
        this.token = token;
        this.tokenSecret = tokenSecret;


    }

    @JsonProperty(PropertyNameConstants.TWEET_QUERY_LIST)
    public List<String> getKeywordList() {
        return this.keywordList;
    }

    @JsonProperty(PropertyNameConstants.TWEET_LOCATION_LIST)
    public String getLocationList() {
        return this.locationList;
    }

    @JsonProperty(PropertyNameConstants.TWEET_NUM)
    public int getTweetNum() {
        return this.tweetNum;
    }

    @JsonProperty(PropertyNameConstants.TWEET_LANGUAGE_LIST)
    public List<String> getLanguageList() {
        return this.languageList;
    }

    @JsonProperty(PropertyNameConstants.TWEET_CUSTOMER_KEY)
    public String getCustomerKey() {return this.customerKey; }

    @JsonProperty(PropertyNameConstants.TWEET_CUSTOMER_SECRET)
    public String getCustomerSecret() {return this.customerSecret; }

    @JsonProperty(PropertyNameConstants.TWEET_TOKEN)
    public String getToken() {return this.token; }

    @JsonProperty(PropertyNameConstants.TWEET_TOKEN_SECRET)
    public String getTokenSecret() {return this.tokenSecret; }

    @Override
    public TwitterFeedOperator newOperator() {
        return new TwitterFeedOperator(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: TwitterFeed")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Obtain real-time tweets using Twitter API")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }

}
