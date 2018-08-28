package edu.uci.ics.texera.dataflow.nlp.sentiment;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

public class MLSentimentTestConstants {
    public static String TEXT = "text";

    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);

    public static Schema SENTIMENT_SCHEMA = new Schema(TEXT_ATTRIBUTE);

    public static Tuple POSITIVE_TUPLE = new Tuple(SENTIMENT_SCHEMA, new TextField("neg"));

    public static Tuple NEGATIVE_TUPLE = new Tuple(SENTIMENT_SCHEMA, new TextField("Bugs are always annoying."));
}
