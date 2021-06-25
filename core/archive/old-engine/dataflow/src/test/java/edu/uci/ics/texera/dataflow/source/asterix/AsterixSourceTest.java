package edu.uci.ics.texera.dataflow.source.asterix;

import java.util.ArrayList;
import java.util.List;


import edu.uci.ics.texera.api.tuple.Tuple;

public class AsterixSourceTest {
	
    public static void test1() {
        AsterixSourcePredicate predicate = new AsterixSourcePredicate(
                "asterixJsonResult", "localhost", 19002, "twitter", "ds_tweet", "text", "zika", "2000-01-01", "2017-05-18", 2);
        AsterixSource asterixSource = predicate.newOperator();
        
        asterixSource.open();
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        while ((tuple = asterixSource.getNextTuple()) != null) {
        	results.add(tuple);
        }
        asterixSource.close();
        
        System.out.println(results);
    }

}
