package edu.uci.ics.texera.dataflow.source.twitter;

import org.junit.Test;

import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverter;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterPredicate;

/**
 * Test cases for operator {@link TwitterConverter}, 
 *  which converts the JSON string representation of twitter to Texera fields.
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterConverterTest {
    
    @Test
    public void testTwitterConverter() {
        
        TwitterConverter twitterConverter = new TwitterConverterPredicate("twitterJson").newOperator();
        
    }

}
