package edu.uci.ics.texera.perftest.twitter;

import java.io.File;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.dataflow.ISourceOperator;
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
import edu.uci.ics.texera.dataflow.twitter.TwitterJsonConverterPredicate;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * Creates an twitter_sample table and ingests a small sample set of twitter data into the table.
 * 
 * @author Zuozhi Wang
 */
public class TwitterSample {
    
    public static String twitterFilePath = PerfTestUtils.getResourcePath("/sample-data-files/twitter/tweets.json").toString();
    public static String twitterClimateTable = "twitter_sample";
    
    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    /**
     * Writes the sample twitter data into the twitter_sample table
     * @throws Exception
     */
    public static void writeTwitterIndex() throws Exception {

        // read the JSON file into a list of JSON string tuples
        JsonNode jsonNode = new ObjectMapper().readTree(new File(twitterFilePath));
        ArrayList<Tuple> jsonStringTupleList = new ArrayList<>();
        
        Schema tupleSourceSchema = new Schema(new Attribute("twitterJson", AttributeType.STRING));
        
        for (JsonNode tweet : jsonNode) {
            Tuple tuple = new Tuple(tupleSourceSchema, new StringField(tweet.toString()));
            jsonStringTupleList.add(tuple);
        }
        
        // setup the twitter converter DAG
        // TupleSource --> TwitterJsonConverter --> TupleSink
        TupleSourceOperator tupleSource = new TupleSourceOperator(jsonStringTupleList, tupleSourceSchema, false);
        
        
        createTwitterTable(twitterClimateTable, tupleSource);
    }
    
    
    /**
     * A helper function to create a table and write twitter data into it.
     * 
     * @param tableName
     * @param twitterJsonSourceOperator, a source operator that provides the input raw twitter JSON string tuples
     * @return
     */
    public static int createTwitterTable(String tableName, ISourceOperator twitterJsonSourceOperator) {
        
        TwitterJsonConverter twitterJsonConverter = new TwitterJsonConverterPredicate("twitterJson").newOperator();

        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        twitterJsonConverter.setInputOperator(twitterJsonSourceOperator);
        tupleSink.setInputOperator(twitterJsonConverter);

        // open the workflow plan and get the output schema
        tupleSink.open();
        
        // create the table with TupleSink's output schema
        RelationManager relationManager = RelationManager.getInstance();
        
        if (relationManager.checkTableExistence(tableName)) {
            relationManager.deleteTable(tableName);
        }
        relationManager.createTable(tableName, Utils.getDefaultIndexDirectory().resolve(tableName),
                tupleSink.getOutputSchema(), LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(tableName);
        dataWriter.open();
        
        Tuple tuple;
        int counter = 0;
        while ((tuple = tupleSink.getNextTuple()) != null) {
            dataWriter.insertTuple(tuple);
            counter++;
        }

        dataWriter.close();
        tupleSink.close();
        
        return counter;
    }

}
