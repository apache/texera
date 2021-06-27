package edu.uci.ics.texera.dataflow.twitterfeed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.httpclient.BasicClient;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import static edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils.TwitterSchema.TEXT;
import static edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils.TwitterSchema.USER_SCREEN_NAME;
import static edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils.TwitterSchema.MEDIA_LINK;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by Chang on 7/13/17.
 */

/**
 * To test the TwitterFeedOperator in two scenarios:
 * First one normally connects the operator to twitter through twitter streaming API.
 * Specify the keywordList, locationList, languageList and number of tweets requested.
 * Check if the number of tweets received is identical to the setting.
 * Check if the query keywords in the keywordList appear in the text and user_screen_name;
 * Check if the location of the received tweets is inside the geo-box defined in the locationList.
 * Check if Limit = 0 for the TwitterFeed Operator can prevent it from receiving any tweet.
 *
 * Second one uses Mockito to mock the TwitterConnector for isolation,
 * and tests the TwitterFeedOperator alone.
 */
public class TwitterFeedOperatorTest {

    private static List<Tuple> exactResults = new ArrayList<>();
    private static final List<String> keywordList = new ArrayList<>(Arrays.asList("day"));
    private static final List<String> languageList = new ArrayList<>(Arrays.asList("en"));
    private static final String locationUS = "25, -123.0, 49.0, -65.0";
    private static int numTweets = 5;

    private final String inputStream = "{\"created_at\":\"Wed Jul 19 19:54:19 +0000 2017\",\"id\":887762546807197702,\"id_str\":\"887762546807197702\",\"text\":\"RT @NoahCRothman: Wow. How times change. RNC reportedly won't back Guadagno because she's \\\"disloyal\\\" to Trump, criticized Access tape. http\\u2026\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":3015370299,\"id_str\":\"3015370299\",\"name\":\"Michelita \\ud83c\\udfaf\",\"screen_name\":\"tripletangels3\",\"location\":null,\"url\":null,\"description\":\"Politically Homeless. #VotedNeither You idiots own the train wreck I don't. Remember you reap what you sow and paybacks are a BITCH! Lists=BLOCKED Spam=REPORTED\",\"protected\":false,\"verified\":false,\"followers_count\":991,\"friends_count\":972,\"listed_count\":10,\"favourites_count\":318497,\"statuses_count\":147118,\"created_at\":\"Tue Feb 03 21:29:34 +0000 2015\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/815132074013261824\\/rPfD-QRa_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/815132074013261824\\/rPfD-QRa_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/3015370299\\/1493677175\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweeted_status\":{\"created_at\":\"Wed Jul 19 18:14:47 +0000 2017\",\"id\":887737498356658177,\"id_str\":\"887737498356658177\",\"text\":\"Wow. How times change. RNC reportedly won't back Guadagno because she's \\\"disloyal\\\" to Trump, criticized Access tape. https:\\/\\/t.co\\/qKx1XS5nQR\",\"source\":\"\\u003ca href=\\\"https:\\/\\/about.twitter.com\\/products\\/tweetdeck\\\" rel=\\\"nofollow\\\"\\u003eTweetDeck\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":168531961,\"id_str\":\"168531961\",\"name\":\"Noah Rothman\",\"screen_name\":\"NoahCRothman\",\"location\":\"New York City\",\"url\":\"http:\\/\\/www.commentarymagazine.com\",\"description\":\"Associate Editor @Commentary. Opinions are my own. Re-tweets not endorsements. Profile photo via my 2-year-old. [Nrothman At Commentarymagazine Dot Com]\",\"protected\":false,\"verified\":true,\"followers_count\":38090,\"friends_count\":1972,\"listed_count\":1348,\"favourites_count\":33421,\"statuses_count\":222870,\"created_at\":\"Mon Jul 19 16:32:51 +0000 2010\",\"utc_offset\":-14400,\"time_zone\":\"Eastern Time (US & Canada)\",\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"022330\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme15\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme15\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"A8C7F7\",\"profile_sidebar_fill_color\":\"C0DFEC\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/831128894703017984\\/uVGFGXnZ_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/831128894703017984\\/uVGFGXnZ_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/168531961\\/1499980657\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":117,\"favorite_count\":110,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/qKx1XS5nQR\",\"expanded_url\":\"http:\\/\\/thehill.com\\/blogs\\/blog-briefing-room\\/news\\/342730-rnc-wont-back-new-jersey-governor-candidate-over-loyalty-report\",\"display_url\":\"thehill.com\\/blogs\\/blog-bri\\u2026\",\"indices\":[117,140]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\"},\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"NoahCRothman\",\"name\":\"Noah Rothman\",\"id\":168531961,\"id_str\":\"168531961\",\"indices\":[3,16]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1500494059646\"}";
    private final int timeOut = 10;
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    /**
     * Prepare outputTuple in advance for the following test.
     * @throws Exception
     */

    @BeforeClass
    public static void prepareTuple() throws Exception {
        exactResults = TwitterFeedTestHelper.getQueryResults(keywordList, locationUS, languageList, numTweets);
    }



    /**
     * Test if the size of generated tupleList is identical to the number of Tweets required.
     * @throws Exception
     */
    @Test
    public void testTwitterFeedLimit() throws Exception {
        Assert.assertEquals(numTweets, exactResults.size());

    }

    /**
     * Test if the query keywords appear in the specified attributes.
     * In twitter streaming API, trackTerms on keyword searches
     * "text", "user_screen_name", "displayed_url" and "expended_url".
     * Since the output schema only includes two of them,
     * containsFuzzyQuery returns true if any tuple contains the keyword
     * in either of these two attributes.
     * @throws Exception
     */

    @Test
    public void testKeywordQuery() throws Exception {
        List<String> searchableAttributes = new ArrayList<>(Arrays.asList(TEXT, USER_SCREEN_NAME, MEDIA_LINK));
        Assert.assertTrue(TwitterFeedTestHelper.checkKeywordInAttributes(exactResults, keywordList, searchableAttributes));

    }


    /**
     * This is to test if the tweets received are inside the geo-box defined in List</Location>.
     * @throws Exception
     */
    @Test
    public void testLocation() throws Exception {
        Assert.assertTrue(TwitterFeedTestHelper.inLocation(exactResults, locationUS));
    }


    @Test
    public void testZeroLimit() throws Exception {
        int limit = 0;
        List<Tuple> exactResults = TwitterFeedTestHelper.getQueryResults(keywordList, null, null, limit);
        assertTrue(exactResults.isEmpty());
    }

    /**
     * Mock the TwitterConnector class and the BasicClient class inside it to test the TwitterFeedOperator alone.
     * Use the pre-defined queue with a Json formatted tweet to generate a tuple.
     * Check if the tuple is well-formatted.
     */
    @Test
    public void testWithMockClient() throws Exception {
        TwitterConnector mockTwitterConnector = mock(TwitterConnector.class);
        queue.add(inputStream);
        TwitterFeedSourcePredicate predicate = new TwitterFeedSourcePredicate(1, keywordList, "", null, null, null, null, null);
        TwitterFeedOperator operator = new TwitterFeedOperator(predicate, mockTwitterConnector);
        operator.setTimeout(timeOut);
        BasicClient mockClient = mock(BasicClient.class);
        when(mockTwitterConnector.getClient()).thenReturn(mockClient);
        when(mockTwitterConnector.getMsgQueue()).thenReturn(queue);
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(operator);
        tupleSink.open();
        List<Tuple> exactResults = tupleSink.collectAllTuples();
        tupleSink.close();
        JsonNode tweet = new ObjectMapper().readValue(inputStream, JsonNode.class);
        Tuple expectedTuple = new Tuple(TwitterUtils.TwitterSchema.TWITTER_SCHEMA,
                new TextField(TwitterUtils.getText(tweet)),
                new StringField(TwitterUtils.getMediaLink(tweet)),
                new StringField(TwitterUtils.getTweetLink(tweet)),
                new StringField(TwitterUtils.getUserLink(tweet)),
                new TextField(TwitterUtils.getUserScreenName(tweet)),
                new TextField(TwitterUtils.getUserName(tweet)),
                new TextField(TwitterUtils.getUserDescription(tweet)),
                new IntegerField(TwitterUtils.getUserFollowerCnt(tweet)),
                new IntegerField(TwitterUtils.getUserFriendsCnt(tweet)),
                new TextField(TwitterUtils.getUserLocation(tweet)),
                new StringField(TwitterUtils.getCreateTime(tweet)),
                new TextField(TwitterUtils.getPlaceName(tweet)),
                new StringField(TwitterUtils.getCoordinates(tweet)),
                new StringField(TwitterUtils.getLanguage(tweet)));

        String exactID = exactResults.get(0).getFields().get(0).getValue().toString();
        String expectedID = exactResults.get(0).getField(SchemaConstants._ID).getValue().toString();

        Assert.assertEquals(exactResults.size(), 1);
        Assert.assertEquals(exactID, expectedID);
        Assert.assertTrue(TwitterFeedTestHelper.compareTuple(exactResults, expectedTuple));

    }

}
