package edu.uci.ics.textdb.exp.twitterfeed;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Chang on 7/12/17.
 */
public class TwitterClient {

         private BasicClient client;
         private BlockingQueue<String> messageQueue;

        public TwitterClient(List<String> query, List<Location> locations, List<String> languages) throws TextDBException{
            if((query == null || query.isEmpty()) && (locations == null || locations.isEmpty())
                    && (languages == null || languages.isEmpty())) {
                throw new DataFlowException("no filter is provided");
            }
            messageQueue = new LinkedBlockingQueue<String>(10000);
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            if(!(query == null || query.isEmpty())){
                endpoint.trackTerms(query);
            }
            if(!(locations == null || locations.isEmpty())){
                endpoint.locations(locations);
            }
            if(!(languages == null || languages.isEmpty())){
                endpoint.languages(languages);
            }

            Authentication auth = new OAuth1(TwitterUtils.twitterConstant.consumerKey, TwitterUtils.twitterConstant.consumerSecret, TwitterUtils.twitterConstant.token, TwitterUtils.twitterConstant.tokenSecret);

            client = new ClientBuilder()
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint)
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(messageQueue))
                    .build();

            //client.connect();
        }
        public BasicClient getClient() {
            return client;
        }

        public void setMessageQueue(BlockingQueue queue){
            this.messageQueue = queue;
        }
        public BlockingQueue<String> getMsgQueue() {
        return messageQueue;
    }

}
