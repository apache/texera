package edu.uci.ics.textdb.exp.twitterfeed;


import com.twitter.hbc.core.endpoint.Location;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import org.json.JSONArray;
import org.json.JSONObject;
import com.twitter.hbc.core.endpoint.Location.Coordinate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Chang on 7/11/17.
 */
public class TwitterUtils {

    public static String getCoordinates(JSONObject object) {
        if (!object.has("coordinates") || object.isNull("coordinates")) return "n/a";
        JSONArray coord = object.getJSONObject("coordinates").getJSONArray("coordinates");
        return  String.valueOf(coord.getDouble(0)) + ", " + String.valueOf(coord.getDouble(1));
    }

    public static String getUserName(JSONObject object){
        return object.getJSONObject("user").getString("name");
    }

    public static String getUserScreenName(JSONObject object){
        return object.getJSONObject("user").getString("screen_name");
    }

    public static String getUserDescription(JSONObject object){

        JSONObject user = object.getJSONObject("user");
        if (user.has("description") && !user.isNull("description")) {
            return user.getString("description");
        }
        return "n/a";
    }

    public static int getUserFollowerCnt(JSONObject object){
        return object.getJSONObject("user").getInt("followers_count");
    }

    public static int getUserFriendsCnt(JSONObject object){
        return object.getJSONObject("user").getInt("friends_count");
    }

    public static String getUserLocation(JSONObject object){
        JSONObject user = object.getJSONObject("user");
        if (!user.has("location") || user.isNull("location")) {
            return "n/a";
        }
        return user.getString("location");
    }

    public static String getUserLink(JSONObject object){
        return "https://twitter.com/" + getUserScreenName(object);
    }

    public static String getPlaceName(JSONObject object) {
        if (!object.has("place") || object.isNull("place")) return "n/a";
        JSONObject place = object.getJSONObject("place");
        String placeName = place.getString("full_name");
        String country = place.getString("country");
        return placeName + ", " + country;
    }

    public static String getCreateTime(JSONObject object) {
        return object.getString("created_at");
    }

    public static String getLanguage(JSONObject object) {
        return object.getString("lang");
    }

    public static String getTexts(JSONObject object) {
        return object.getString("text");
    }

    public static String getTweetLink(JSONObject object) {
        String tweetLink = "https://twitter.com/statuses/" + object.getString("id_str");
        return tweetLink;

    }

    public static List<Location> getPlaceLocation(String inputLocation) throws TextDBException {
        List<Location> locationList = new ArrayList<>();
        if(inputLocation == null || inputLocation.isEmpty()){
            return locationList;
        }
        List<String> boudingCoordinate = Arrays.asList(inputLocation.trim().split(","));
        if(boudingCoordinate.size() != 4 || boudingCoordinate.stream().anyMatch(s -> s.trim().isEmpty())){
            throw new DataFlowException("Please provide valid location coordinates");
        }
        List<Double> boundingBox = new ArrayList<>();
        boudingCoordinate.stream().forEach(s -> boundingBox.add(Double.parseDouble(s.trim())));

        locationList.add(new Location(new Coordinate(boundingBox.get(0), boundingBox.get(1)), new Coordinate(boundingBox.get(2), boundingBox.get(3))));
        return locationList;
    }



    public static class twitterSchema {

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


            public static String PROFILE_LOCATION = "profile_location";
            public static Attribute PROFILE_LOCATION_ATTRIBUTE = new Attribute(PROFILE_LOCATION, AttributeType.TEXT);

            public static String CREATE_AT = "create_at";
            public static Attribute CREATE_AT_ATTRIBUTE = new Attribute(CREATE_AT, AttributeType.STRING);

            public static String TWEET_LOCATION = "tweet_place_name";
            public static Attribute TWEET_LOCATION_ATTRIBUTE = new Attribute(TWEET_LOCATION, AttributeType.TEXT);

            public static String TWEET_COORDINATES = "tweet_coordinates";
            public static Attribute TWEET_COORDINATES_ATTRIBUTE = new Attribute(TWEET_COORDINATES, AttributeType.STRING);

            public static String LANGUAGE = "lang";
            public static Attribute LANGUAGE_ATTRIBUTE = new Attribute(LANGUAGE, AttributeType.STRING);

            public static Schema TWITTER_SCHEMA = new Schema(
                    TEXT_ATTRIBUTE, TWEET_LINK_ATTRIBUTE, USER_LINK_ATTRIBUTE,
                    USER_SCREEN_NAME_ATTRIBUTE, USER_NAME_ATTRIBUTE, USER_DESCRIPTION_ATTRIBUTE,
                    USER_FOLLOWERS_COUNT_ATTRIBUTE, USER_FRIENDS_COUNT_ATTRIBUTE,
                    PROFILE_LOCATION_ATTRIBUTE, CREATE_AT_ATTRIBUTE, TWEET_LOCATION_ATTRIBUTE, TWEET_COORDINATES_ATTRIBUTE, LANGUAGE_ATTRIBUTE);


    }

    public static class twitterConstant {
            public static final String consumerKey = "iJI9uxE1EKFlDwWOJnB1nvl2J";
            public static final String consumerSecret = "DjcNacjs9KOwO3w9zfBSWNJF96yerBj3GrgsJaMdERaWrG0a28";
            public static final String token = "884194955031306240-MucmXV5HBt9gFZfJ5WqGJZa11fhNTKT";
            public static final String tokenSecret = "WCEREP8SV6lhTacL1wvlluFkLRZGqBIiCC9fNLjbpM0Lt";
     }

    }




