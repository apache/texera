package edu.uci.ics.textdb.exp.twitterfeed;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.tuple.Tuple;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;


/**
 * Created by Chang on 7/13/17.
 */
public class TwitterFeedTest {

    private TwitterClient client;
    /***
     * Test limit on the number of output tuple from twitter stream API.
     * @throws Exception
     */
    @Test
    public void testLimit1() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("Today"));
        int limit = 2;
        List<Tuple> exactResults = TwitterFeedTestHelper.getQueryResults(query, null, null, limit);
        Assert.assertEquals(exactResults.size(), limit);

    }

    @Test
    public void testLimit2() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("tom and jerry"));
        int limit = 0;
        List<Tuple> exactResults = TwitterFeedTestHelper.getQueryResults(query, null, null, limit);
        assertTrue(exactResults.isEmpty());

    }

    @Test
    public void testKeywordQuery() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("day"));
        int limit = 10;
        List<String> attribute = new ArrayList<>(Arrays.asList("text", "user_screen_name"));
        List<Tuple> exactResults = TwitterFeedTestHelper.getQueryResults(query, null, null, limit);
        Assert.assertTrue(TwitterFeedTestHelper.containsFuzzyQuery(exactResults, query, attribute));

    }

    @Test
    public void testLocaton() throws Exception {
        String NEWYORK = "-74.25909, 40.49137, -73.700272, 40.915256";
        int limit = 2;
        List<Tuple> exactResults = TwitterFeedTestHelper.getQueryResults(null, NEWYORK, null, limit);
        Assert.assertTrue(TwitterFeedTestHelper.inLocation(exactResults, NEWYORK));
    }


    @Before
    public void setUp() throws Exception {
        List<String> keyWord = new ArrayList<>(Arrays.asList("is"));
        client= new TwitterClient(keyWord, null, null);
        client.getClient().connect();

    }

    @After
    public void tearDown() throws Exception {
        client.getClient().stop();
    }

    @Test
    public void testTwitterClient() throws Exception {
        int msg = 0;
        while (msg < 2) {
            String message = client.getMsgQueue().take();
            assertTrue(! TwitterUtils.getUserScreenName(new JSONObject(message)).isEmpty());
            msg++;
        }
    }

}
