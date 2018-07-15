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
import edu.uci.ics.texera.dataflow.twitter.TwitterConverter;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterConstants;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterPredicate;
import junit.framework.Assert;

/**
 * Test cases for operator {@link TwitterConverter}, 
 *  which converts the JSON string representation of twitter to Texera fields.
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterConverterTest {
    
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
        // TupleSource --> TwitterConverter --> TupleSink
        TupleSourceOperator tupleSource = new TupleSourceOperator(jsonStringTupleList, tupleSourceSchema);

        TwitterConverter twitterConverter = new TwitterConverterPredicate("twitterJson").newOperator();

        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        twitterConverter.setInputOperator(tupleSource);
        tupleSink.setInputOperator(twitterConverter);
        
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
        Assert.assertTrue(testTuple.getSchema().getAttributes().containsAll(TwitterConverterConstants.additionalAttributes));
        
        // make sure that all the tuple fields corresponds to the schema
        for (Attribute attr : testTuple.getSchema().getAttributes()) {
            Class<? extends IField> expectedFieldClass = attr.getType().getFieldClass();
            Class<? extends IField> actualFieldClass = testTuple.getField(attr.getName()).getClass();

            Assert.assertEquals(expectedFieldClass, actualFieldClass);
        }
        
    }

}
