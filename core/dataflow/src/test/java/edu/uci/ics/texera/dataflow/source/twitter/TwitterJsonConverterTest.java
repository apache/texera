package edu.uci.ics.texera.dataflow.source.twitter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverter;
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverterConstants;
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverterPredicate;
import junit.framework.Assert;

/**
 * Test cases for operator {@link TwitterJsonConverter},
 *  which converts the JSON string representation of twitter to Texera fields.
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterJsonConverterTest {
    
    // get the sample twitter data from the perf test resources folder
    public static String twitterFilePath =  Utils.getResourcePath("/sample-data-files/twitter/tweets.json", TexeraProject.TEXERA_PERFTEST).toString();

    public static List<Tuple> getAllSampleTwitterTuples() throws Exception {
        
        // read the JSON file into a list of JSON string tuples
        JsonNode jsonNode = new ObjectMapper().readTree(new File(twitterFilePath));
        ArrayList<Tuple> jsonStringTupleList = new ArrayList<>();
        
        Schema tupleSourceSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE, 
                new Attribute("twitterJson", AttributeType.STRING));
        
        for (JsonNode tweet : jsonNode) {
            Tuple tuple = new Tuple(tupleSourceSchema, IDField.newRandomID(), new StringField(tweet.toString()));
            jsonStringTupleList.add(tuple);
        }
        
        // setup the twitter converter DAG
        // TupleSource --> TwitterJsonConverter --> TupleSink
        TupleSourceOperator tupleSource = new TupleSourceOperator(jsonStringTupleList, tupleSourceSchema);

        TwitterJsonConverter twitterJsonConverter = new TwitterJsonConverterPredicate("twitterJson").newOperator();

        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        twitterJsonConverter.setInputOperator(tupleSource);
        tupleSink.setInputOperator(twitterJsonConverter);
        
        tupleSink.open();
        
        List<Tuple> tuples = tupleSink.collectAllTuples();
        
        tupleSink.close();
        
        return tuples;
    }
    
    @Test
    public void testTwitterConverter() throws Exception {
        
        List<Tuple> twitterTuples = getAllSampleTwitterTuples();
        
        Assert.assertTrue(twitterTuples.size() > 0);
        
        Tuple testTuple = twitterTuples.get(0);
        
        // make sure that all the additional attributes are in the schema
        Assert.assertTrue(testTuple.getSchema().getAttributes().containsAll(TwitterJsonConverterConstants.additionalAttributes));
        
    }

}
