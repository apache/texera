package edu.uci.ics.texera.perftest.twitter;

import java.io.File;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.field.IDField;
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
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterPredicate;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class TwitterSample {
    
    public static String twitterFilePath = PerfTestUtils.getResourcePath("/sample-data-files/twitter/tweets.json").toString();
    public static String twitterClimateTable = "twitter_sample";
    
    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    public static void writeTwitterIndex() throws Exception {

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
        
        
        createTwitterTable(twitterClimateTable, tupleSource);
    }
    
    
    public static int createTwitterTable(String tableName, ISourceOperator twitterJsonSourceOperator) {
        
        TwitterConverter twitterConverter = new TwitterConverterPredicate("twitterJson").newOperator();

        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        twitterConverter.setInputOperator(twitterJsonSourceOperator);
        tupleSink.setInputOperator(twitterConverter);

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
