package edu.uci.ics.texera.perftest.twitter;

import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverter;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterPredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class AsterixTwitterIngest {
    
    public static void ingestKeywords(String tableName, String keywords, String startDate, String endDate) {
        RelationManager relationManager = RelationManager.getInstance();
                
        AsterixSourcePredicate asterixSourcePredicate = new AsterixSourcePredicate(
                "actinium.ics.uci.edu",
                19002,
                "twitter",
                "ds_tweet",
                "text",
                keywords,
                startDate,
                endDate,
                null);
        
        AsterixSource asterixSource = asterixSourcePredicate.newOperator();
        
        TwitterConverter twitterConverter = new TwitterConverterPredicate().newOperator();
        
        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        tupleSink.setInputOperator(twitterConverter);
        twitterConverter.setInputOperator(asterixSource);
        
        tupleSink.open();
        
        if (relationManager.checkTableExistence(tableName)) {
            relationManager.deleteTable(tableName);
        }
        relationManager.createTable(tableName, Utils.getDefaultIndexDirectory().resolve(tableName), 
                tupleSink.getOutputSchema(), LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(tableName);
        dataWriter.open();
        
        Tuple tuple;
        while ((tuple = tupleSink.getNextTuple()) != null) {
            dataWriter.insertTuple(tuple);
            System.out.println(tuple);
        }
        
        dataWriter.close();
        tupleSink.close();
    }

}
