package edu.uci.ics.textdb.dataflow.source;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.Collector;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.SampleTuple;

public class Constants {
	public static final String INDEX_DIR = "index";
	public static final String WILD_CARD_QUERY = "*:*";
	public static final int MAX_RESULTS = 100;
	
	//Sample Fields
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    
    //Sample Schema
    public static final List<String> SAMPLE_SCHEMA = Arrays.asList(FIRST_NAME, LAST_NAME);
    
    //Sample Tuples
    public static final List<ITuple> SAMPLE_TUPLES = Arrays.asList(
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f1"), new StringField("l1")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f2"), new StringField("l2")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f3"), new StringField("l3")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f4"), new StringField("l4")),
            new SampleTuple(SAMPLE_SCHEMA, new StringField("f5"), new StringField("l5")));

}
