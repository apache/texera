package edu.uci.ics.texera.dataflow.twitterfeed;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.core.endpoint.Location;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import com.twitter.hbc.core.endpoint.Location.Coordinate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Chang on 7/11/17.
 */


/**
 * Define Json parser functions, twitter schema, twitter constants such as consumer key, token and secret.
 */
public class TwitterUtils {
    private static final String informationNotAvailable = "n/a";

    public static String getCoordinates(JsonNode object) {
        if (object.hasNonNull(twitterConstant.COORDINCATES)) {
            JsonNode coord = object.get(twitterConstant.COORDINCATES).get(twitterConstant.COORDINCATES);
            return String.valueOf(coord.get(0).asDouble()) + ", " + String.valueOf(coord.get(1).asDouble());
        }
        return informationNotAvailable;
    }

    public static String getUserName(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            return object.get(twitterConstant.USER).get(twitterConstant.NAME).asText();
        }
        return informationNotAvailable;
    }

    public static String getUserScreenName(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            return object.get(twitterConstant.USER).get(twitterConstant.SCREENNAME).asText();
        }
        return informationNotAvailable;
    }

    public static String getUserDescription(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            JsonNode user = object.get(twitterConstant.USER);
            if (user.hasNonNull(twitterConstant.DESCRIPTION)) {
                return user.get(twitterConstant.DESCRIPTION).asText();
            }
        }
        return informationNotAvailable;
    }

    public static int getUserFollowerCnt(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            return object.get(twitterConstant.USER).get(twitterConstant.FOLLOWERCNT).asInt();
        }
        return 0;
    }

    public static int getUserFriendsCnt(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            return object.get(twitterConstant.USER).get(twitterConstant.FRIENDCNT).asInt();
        }
        return 0;
    }

    public static String getUserLocation(JsonNode object) {
        if (object.hasNonNull(twitterConstant.USER)) {
            JsonNode user = object.get(twitterConstant.USER);
            if (user.hasNonNull(twitterConstant.LOCATION)) {
                return user.get(twitterConstant.LOCATION).asText();
            }
        }
        return informationNotAvailable;
    }

    public static String getUserLink(JsonNode object) {
        return twitterConstant.TWITTERLINK + getUserScreenName(object);
    }

    public static String getPlaceName(JsonNode object) {
        if (object.hasNonNull(twitterConstant.PLACE)) {
            JsonNode place = object.get(twitterConstant.PLACE);
            String placeName = place.get(twitterConstant.PLACEFULLNAME).asText();
            String country = place.get(twitterConstant.COUNTRY).asText();
            return placeName + ", " + country;
        }
        return informationNotAvailable;
    }

    public static String getCreateTime(JsonNode object) {
        if (object.hasNonNull(twitterConstant.CREATTIME)) {
            return object.get(twitterConstant.CREATTIME).asText();
        }
        return informationNotAvailable;
    }

    public static String getLanguage(JsonNode object) {
        if (object.hasNonNull(twitterConstant.LANGUAGE)) {
            return object.get(twitterConstant.LANGUAGE).asText();
        }
        return informationNotAvailable;
    }

    public static String getText(JsonNode object) {
        if (object.hasNonNull(twitterConstant.TEXT)) {
            return object.get(twitterConstant.TEXT).asText();
        }
        return informationNotAvailable;
    }

    public static String getTweetLink(JsonNode object) {
        if (object.hasNonNull(twitterConstant.TWEETID)) {
            String tweetLink = twitterConstant.TWEETLINK + object.get(twitterConstant.TWEETID).asText();
            return tweetLink;
        }
        return informationNotAvailable;

    }

    /**
     * To track tweets inside a geoBox defined by a List</Location>.
     * The string defines the coordinates in the order of "latitude_SW, longitude_SW, latitude_NE, longitude_NE".
     *
     * @param inputLocation
     * @return
     * @throws TexeraException
     */
    public static List<Location> getPlaceLocation(String inputLocation) throws TexeraException {
        List<Location> locationList = new ArrayList<>();
        if (inputLocation == null || inputLocation.isEmpty()) {
            return locationList;
        }
        List<String> boudingCoordinate = Arrays.asList(inputLocation.trim().split(","));
        if (boudingCoordinate.size() != 4 || boudingCoordinate.stream().anyMatch(s -> s.trim().isEmpty())) {
            throw new DataflowException("Please provide valid location coordinates");
        }
        List<Double> boundingBox = new ArrayList<>();
        boudingCoordinate.stream().forEach(s -> boundingBox.add(Double.parseDouble(s.trim())));

        locationList.add(new Location(new Coordinate(boundingBox.get(1), boundingBox.get(0)), new Coordinate(boundingBox.get(3), boundingBox.get(2))));
        return locationList;
    }

    public static String getMediaLink(JsonNode object) throws IOException {
        if(object.hasNonNull(twitterConstant.ENTITY)) {
            ObjectMapper mapper = new ObjectMapper();
            List<JsonNode> List = mapper.readValue(object.get(twitterConstant.ENTITY).get(twitterConstant.URLS).toString(), mapper.getTypeFactory().constructCollectionType(List.class, JsonNode.class));
            if(List.isEmpty()) {
                return informationNotAvailable;
            }
            for (JsonNode jn : List) {
                String display_URL = jn.get(twitterConstant.DISPLAYURL).toString();
                String expanded_URL = jn.get(twitterConstant.EXPANDEDURL).toString();
                return display_URL + "\n" + expanded_URL;
            }
        }
        return informationNotAvailable;
    }

    /**
     * Tweet Sample in Json format:
     * text : "@Toni090902 Hi, I'm here to make you feel good every day about your decision to follow me, or is that regret it, I forget :/"
     * screen_name : "SocialMedia_RS"
     * name : "SocialMedia Rockstar"
     * description : "I am the Great White Elephant of Social Media, a true Rockstar. When I talk you should listen. Ent.account of @HOLMedia don't bother DM'ing, I don't do that"
     * followers_count : 43541
     * friends_count : 29236
     * location : "Tampa, FL"
     * created_at : "Thu Jun 16 20:21:48 +0000 2011"
     * place : {
     * "id": "fd70c22040963ac7",
     * "url": "https:\/\/api.twitter.com\/1.1\/geo\/id\/fd70c22040963ac7.json",
     * "place_type": "city",
     * "name": "Boulder",
     * "full_name": "Boulder, CO",
     * "country_code": "US",
     * "country": "United States",
     * "contained_within": [
     * ],
     * "bounding_box": {
     * "type": "Polygon",
     * "coordinates": [
     * [
     * [-105.301758, 39.964069],
     * [-105.301758, 40.094551],
     * [-105.178142, 40.094551],
     * [-105.178142, 39.964069]
     * ]
     * ]
     * },
     * "attributes": {}
     * }
     * coordinates : {
     * "type": "Point",
     * "coordinates": [-105.2812196, 40.0160921]
     * }
     * lang : "en"
     */

    public static class TwitterSchema {

        public static String TEXT = "text";
        public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);

        public static String MEDIA_LINK = "media_link";
        public static Attribute MEDIA_LINK_ATTRIBUTE = new Attribute(MEDIA_LINK, AttributeType.STRING);

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
                TEXT_ATTRIBUTE, MEDIA_LINK_ATTRIBUTE, TWEET_LINK_ATTRIBUTE, USER_LINK_ATTRIBUTE,
                USER_SCREEN_NAME_ATTRIBUTE, USER_NAME_ATTRIBUTE, USER_DESCRIPTION_ATTRIBUTE,
                USER_FOLLOWERS_COUNT_ATTRIBUTE, USER_FRIENDS_COUNT_ATTRIBUTE,
                PROFILE_LOCATION_ATTRIBUTE, CREATE_AT_ATTRIBUTE, TWEET_LOCATION_ATTRIBUTE, TWEET_COORDINATES_ATTRIBUTE, LANGUAGE_ATTRIBUTE);


    }

    /**
     * Defines consumerKey and token for twitterClient connection.
     */

    public static class twitterConstant {

        public static final String consumerKey = "iJI9uxE1EKFlDwWOJnB1nvl2J";
        public static final String consumerSecret = "DjcNacjs9KOwO3w9zfBSWNJF96yerBj3GrgsJaMdERaWrG0a28";
        public static final String token = "884194955031306240-MucmXV5HBt9gFZfJ5WqGJZa11fhNTKT";
        public static final String tokenSecret = "WCEREP8SV6lhTacL1wvlluFkLRZGqBIiCC9fNLjbpM0Lt";
        private static final String PLACE = "place";
        private static final String COORDINCATES = "coordinates";
        private static final String USER = "user";
        private static final String PLACEFULLNAME = "full_name";
        private static final String COUNTRY = "country";
        private static final String NAME = "name";
        private static final String SCREENNAME = "screen_name";
        private static final String DESCRIPTION = "description";
        private static final String FOLLOWERCNT = "followers_count";
        private static final String FRIENDCNT = "friends_count";
        private static final String LOCATION = "location";
        private static final String CREATTIME = "created_at";
        private static final String LANGUAGE = "language";
        private static final String TEXT = "text";
        private static final String TWEETLINK = "https://twitter.com/statuses/";
        private static final String TWEETID = "id_str";
        private static final String TWITTERLINK = "https://twitter.com/";
        private static final String ENTITY = "entities";
        private static final String URLS = "urls";
        private static final String DISPLAYURL = "display_url";
        private static final String EXPANDEDURL = "expanded_url";
    }

}




