package edu.uci.ics.textdb.exp.nlp.sentiment;

import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class NlpSentimentTestConstants {
    
    public static String TEXT = "text";
    
    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);
    
    public static Schema SENTIMENT_SCHEMA = new Schema(TEXT_ATTRIBUTE);
    
    public static Tuple POSITIVE_TUPLE = new Tuple(SENTIMENT_SCHEMA, 
            new TextField("Programming is so super awesome."));
    
    public static Tuple NEUTRAL_TUPLE = new Tuple(SENTIMENT_SCHEMA,
            new TextField("TextDB uses Java."));
    
    public static Tuple NEGATIVE_TUPLE = new Tuple(SENTIMENT_SCHEMA,
            new TextField("Bugs are always annoying."));
    
}
