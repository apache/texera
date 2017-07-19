package edu.uci.ics.textdb.exp.twitterfeed;

/**
 * Created by Chang on 7/11/17.
 */




import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import org.json.JSONObject;
import org.mockito.Mockito.*;
import static edu.uci.ics.textdb.exp.twitterfeed.TwitterUtils.twitterSchema.TWEET_COORDINATES;
import static org.mockito.Mockito.mock;

public class SampleTrack {
    private static InputStream mockInputStream;
    //private InputStream mockInputStream;

        public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
            BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            String s = "-74.25909, 40.49137, -73.700272, 40.915256";
             List<Location> l = TwitterUtils.getPlaceLocation(s);

                // add some track terms
            endpoint.trackTerms(Lists.newArrayList("trump team"));
            endpoint.languages(new ArrayList<String>(Arrays.asList("en")));
            endpoint.locations(l);

            //endpoint.locations(Lists.newArrayList("China"));
            //BasicConfigurator.configure();

            Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
            // Authentication auth = new BasicAuth(username, password);

            // Create a new BasicClient. By default gzip is enabled.
           BasicClient client = new ClientBuilder()
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint)
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build();

           Authentication auth1 = mock(Authentication.class);
           StatusesFilterEndpoint endpont1 = mock(StatusesFilterEndpoint.class);
           StringDelimitedProcessor processor = mock(StringDelimitedProcessor.class);
           mockInputStream = mock(InputStream.class);
           String ss;
            processor.setup(mockInputStream);

            // Do whatever needs to be done with messages
//            for (int msgRead = 0; msgRead < 10; msgRead++) {
//                StatsReporter.StatsTracker output = client1.getStatsTracker();
//                String msg = queue.take();
//                JSONObject tweet = new JSONObject(msg);
//
//                String createTime = TwitterUtils.getCreateTime(tweet);
//                if( (tweet.getJSONObject("entities").getJSONArray("urls") != null) && (tweet.getJSONObject("entities").getJSONArray("urls").length() != 0)){
//                    Object displayurl = tweet.getJSONObject("entities").getJSONArray("urls").get(0);
//                    String re = displayurl.toString();
//                    System.out.println(re);
//                }


                //JSONObject displayurl = (JSONObject) tweet.getJSONObject("entities").getJSONArray("urls").get(0);
               // String re = displayurl.getString("display_url");


//                System.out.println("Created at: " + createTime);
//                System.out.println("name " + TwitterUtils.getUserName(tweet));
//                System.out.println("location " + TwitterUtils.getUserLocation(tweet));
//                System.out.println("Text" + TwitterUtils.getTexts(tweet));
//                System.out.println("Language" + TwitterUtils.getLanguage(tweet));
//                  System.out.println("TweetLink " + TwitterUtils.getTweetLink(tweet));
//                System.out.println("UserLink " + TwitterUtils.getUserLink(tweet));
//                System.out.println("Description " + TwitterUtils.getUserDescription(tweet));
//                System.out.println("UserFollower " + TwitterUtils.getUserFollowerCnt(tweet));
//                System.out.println("UserFriend " + TwitterUtils.getUserFriendsCnt(tweet));
//                System.out.println("PlaceName" + TwitterUtils.getPlaceName(tweet));
//                System.out.println(TwitterUtils.getCoordinates(tweet));
//
//
//                System.out.println(msg);
//            }

            client.stop();
            System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());

        }

    public static void main(String[] args) throws InterruptedException {
            String consumerKey = TwitterUtils.twitterConstant.consumerKey;
            String consumerSecret = TwitterUtils.twitterConstant.consumerSecret;
            String token = TwitterUtils.twitterConstant.token;
            String tokenSecret = TwitterUtils.twitterConstant.tokenSecret;
            SampleTrack.run(consumerKey, consumerSecret, token, tokenSecret);
            String s = "-74.25909, 40.49137, -73.700272, 40.915256";
           // Location l = TwitterUtils.getPlaceLocation(s);
          //  System.out.println(l.northeastCoordinate().toString());
          //  System.out.println(l.southwestCoordinate().toString());

    }

//       public static void main(String[] args){
//            List<String> keywordList = new ArrayList<>();
//            //keywordList.add("Trump");
//           // keywordList.add("Trump");
//            String s = "-74.25909, 40.49137, -73.700272, 40.915256";
//           TwitterFeedSourcePredicate predicate = new TwitterFeedSourcePredicate(10, keywordList, s, null);
//           TwitterFeedOperator twitterFeedOperator = new TwitterFeedOperator(predicate);
//           twitterFeedOperator.setLimit(20);
//           twitterFeedOperator.setTimeout(20);
//           TupleSink tupleSink = new TupleSink();
//           tupleSink.setInputOperator(twitterFeedOperator);
//           tupleSink.open();
//           List<Tuple> result = tupleSink.collectAllTuples();
//           for(Tuple t: result){
//               System.out.println(" this is id" + t.getField(SchemaConstants._ID).getValue().toString());
//               System.out.println(t.getField("text").getValue().toString());
//               System.out.println(t.getField("user_screen_name").getValue().toString());
//               System.out.println(t.getField("tweet_coordinates").getValue().toString());
//               System.out.println(t.getField("tweet_place_name").getValue().toString());
////               System.out.println(t.getField("Location").toString());
//              // System.out.println(t.getField(TWEET_LINK).toString());
//               // System.out.println(t.getField("abstract").toString());
//
//           }
//
//           boolean c = containsQuery(result, keywordList, "text");
//           System.out.println(result.size());
//
//       }

    public static boolean containsQuery(List<Tuple> result, List<String> query, String attribute) {
        for (Tuple tuple : result) {
            List<String> toMatch = query.stream()
                    .filter(s -> tuple.getField(attribute).getValue().toString().toLowerCase().contains(s.toLowerCase()))
                    .collect(Collectors.toList());
            if(toMatch.isEmpty()) return false;

        }
        return true;
    }
    public static boolean containsFuzzyQuery(List<Tuple> result, List<String> query, String attribute) {
        for (Tuple tuple : result) {
            List<String> toMatch = query.stream()
                    .filter(s -> tuple.getField(attribute).getValue().toString().toLowerCase().contains(s.toLowerCase()))
                    .collect(Collectors.toList());
            if(toMatch.isEmpty()) return false;

        }
        return true;
    }

}
