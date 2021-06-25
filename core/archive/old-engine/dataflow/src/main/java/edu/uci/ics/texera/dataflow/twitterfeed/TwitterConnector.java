package edu.uci.ics.texera.dataflow.twitterfeed;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Chang on 7/12/17.
 */

/**
 * This class is to set up a connection with Twitter using the twitter streaming API.
 * Details of this API: https://dev.twitter.com/streaming/overview
 * Twitter streaming API supports "trackTerms" on keyword, location and language.
 * These three query domains can not be empty at the same time.
 */
public class TwitterConnector {

    private BasicClient twitterClient;
    private BlockingQueue<String> messageQueue;
    private int queueSize = 10000;
    private Authentication auth;

    public TwitterConnector(List<String> keywordList, List<Location> locationList, List<String> languageList, String customerKey, String customerSecret, String token, String tokenSecret) throws TexeraException {
        if ((keywordList == null || keywordList.isEmpty()) && (locationList == null || locationList.isEmpty())
                && (languageList == null || languageList.isEmpty())) {
            throw new DataflowException("no filter is provided");
        }

        //Set up the blocking queue with proper size based on expected TPS of the stream.
        messageQueue = new LinkedBlockingQueue<String>(queueSize);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        if (!(keywordList == null || keywordList.isEmpty())) {
            endpoint.trackTerms(keywordList);
        }
        if (!(locationList == null || locationList.isEmpty())) {
            endpoint.locations(locationList);
        }
        if (!(languageList == null || languageList.isEmpty())) {
            endpoint.languages(languageList);
        }

        if ((customerKey != null && !customerKey.isEmpty()) && (customerSecret != null && !customerSecret.isEmpty()) && (token != null && !token.isEmpty()) && (tokenSecret != null && !tokenSecret.isEmpty())) {
            auth = new OAuth1(customerKey, customerSecret, token, tokenSecret);
        } else {
            auth = new OAuth1(TwitterUtils.twitterConstant.consumerKey, TwitterUtils.twitterConstant.consumerSecret, TwitterUtils.twitterConstant.token, TwitterUtils.twitterConstant.tokenSecret);
        }


        //BasicClient sets up connection to twitter with declaration on hostname to connect to, endpoint with trackTerms, authentication information and processor to process client events.
        twitterClient = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(messageQueue))
                .build();
    }

    public BasicClient getClient() {
        return twitterClient;
    }

    public void setMessageQueue(BlockingQueue<String> queue) {
        this.messageQueue = queue;
    }

    public BlockingQueue<String> getMsgQueue() {
        return messageQueue;
    }

}
