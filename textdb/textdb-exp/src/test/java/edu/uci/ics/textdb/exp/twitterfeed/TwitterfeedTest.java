package edu.uci.ics.textdb.exp.twitterfeed;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.tuple.Tuple;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;


/**
 * Created by Chang on 7/13/17.
 */
public class TwitterfeedTest {
    /***
     * Test limit on the number of output tuple from twitter stream API.
     * @throws Exception
     */
    @Test
    public void testLimit1() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("Today"));
        int limit = 2;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(query, null, null, limit);
        Assert.assertEquals(exactResults.size(), limit);

    }

    @Test
    public void testLimit2() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("tom and jerry"));
        int limit = 0;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(query, null, null, limit);
        assertTrue(exactResults.isEmpty());

    }

    /***
     * Test tuple with ID.
     * @throws Exception
     */
    @Test
    public void testSchemaID() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("trump"));
        int limit = 1;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(query, null, null, limit);
        String exactID = exactResults.get(0).getField(0).getValue().toString();
        String expectedID = exactResults.get(0).getField(SchemaConstants._ID).getValue().toString();
        Assert.assertEquals(exactResults.size(), limit);
        Assert.assertEquals(exactID, expectedID);

    }

    @Test
    public void testKeywordQuery() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("Trump team"));
        int limit = 10;
        List<String> attribute = new ArrayList<>(Arrays.asList("text", "user_screen_name"));
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(query, null, null, limit);
      //  System.out.println(exactResults.get(0).getField("text").getValue().toString());
        Assert.assertTrue(TwitterfeedTestHelper.containsFuzzyQuery(exactResults, query, attribute));

    }

    @Test
    public void testLanguage() throws Exception {
        List<String> lang = new ArrayList<>(Arrays.asList("en"));
        List<String> attribute = new ArrayList<>(Arrays.asList("lang"));
        int limit = 5;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(null, null, lang, limit);
        Assert.assertTrue(TwitterfeedTestHelper.containsQuery(exactResults, lang, attribute));

    }
    @Test
    public void testSchemaNum() throws Exception {
        List<String> query = new ArrayList<>(Arrays.asList("hello"));
        int limit = 10;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(query, null, null, limit);
        Assert.assertTrue(TwitterfeedTestHelper.schemaNums(exactResults));
    }

    @Test
    public void testLocaton() throws Exception {
        String NEWYORK = "-74.25909, 40.49137, -73.700272, 40.915256";
        int limit = 2;
        List<Tuple> exactResults = TwitterfeedTestHelper.getQueryResults(null, NEWYORK, null, limit);
        Assert.assertTrue(TwitterfeedTestHelper.inLocation(exactResults, NEWYORK));
    }


}
