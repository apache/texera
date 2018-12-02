package edu.uci.ics.texera.dataflow.twitter;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;

public class TwitterJsonConverterConstants {
    
    public static String TWEET_ID = "tweet_id";
    public static Attribute TWEET_ID_ATTRIBUTE = new Attribute(TWEET_ID, AttributeType.STRING);
    
    public static String TEXT = "text";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);
    
    public static String TWEET_LINK = "tweet_link";
    public static Attribute TWEET_LINK_ATTRIBUTE = new Attribute(TWEET_LINK, AttributeType.STRING);
    
    public static String USER_LINK = "user_link";
    public static Attribute USER_LINK_ATTRIBUTE = new Attribute(USER_LINK, AttributeType.STRING);
    
    public static String USER_SCREEN_NAME = "user_screen_name";
    public static Attribute USER_SCREEN_NAME_ATTRIBUTE = new Attribute(USER_SCREEN_NAME, AttributeType.TEXT);
    
    public static String USER_NAME = "user_name";
    public static Attribute USER_NAME_ATTRIBUTE = new Attribute(USER_NAME, AttributeType.TEXT);
    
    public static String USER_DESCRIPTION = "user_description";
    public static Attribute USER_DESCRIPTION_ATTRIBUTE = new Attribute(USER_DESCRIPTION, AttributeType.TEXT);
    
    public static String USER_FOLLOWERS_COUNT = "user_followers_count";
    public static Attribute USER_FOLLOWERS_COUNT_ATTRIBUTE = new Attribute(USER_FOLLOWERS_COUNT, AttributeType.INTEGER);
    
    public static String USER_FRIENDS_COUNT = "user_friends_count";
    public static Attribute USER_FRIENDS_COUNT_ATTRIBUTE = new Attribute(USER_FRIENDS_COUNT, AttributeType.INTEGER);
    
    public static String STATE = "state";
    public static Attribute STATE_ATTRIBUTE = new Attribute(STATE, AttributeType.TEXT);
    
    public static String COUNTY = "county";
    public static Attribute COUNTY_ATTRIBUTE = new Attribute(COUNTY, AttributeType.TEXT);
    
    public static String CITY = "city";
    public static Attribute CITY_ATTRIBUTE = new Attribute(CITY, AttributeType.TEXT);
    
    public static String CREATE_AT = "create_at";
    public static Attribute CREATE_AT_ATTRIBUTE = new Attribute(CREATE_AT, AttributeType.DATETIME);
    
    public static String IS_RETWEET = "is_retweet";
    public static Attribute IS_RETWEET_ATTRIBUTE = new Attribute(IS_RETWEET, AttributeType.STRING);

    
    public static List<Attribute> additionalAttributes = Arrays.asList(
            TWEET_ID_ATTRIBUTE, TEXT_ATTRIBUTE, TWEET_LINK_ATTRIBUTE, USER_LINK_ATTRIBUTE, 
            USER_SCREEN_NAME_ATTRIBUTE, USER_NAME_ATTRIBUTE, USER_DESCRIPTION_ATTRIBUTE, 
            USER_FOLLOWERS_COUNT_ATTRIBUTE, USER_FRIENDS_COUNT_ATTRIBUTE, 
            STATE_ATTRIBUTE, COUNTY_ATTRIBUTE, CITY_ATTRIBUTE, CREATE_AT_ATTRIBUTE, IS_RETWEET_ATTRIBUTE);

}