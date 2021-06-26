package edu.uci.ics.texera.dataflow.sink.wordcloud;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;


public class WordCloudSinkTestConstants {
    public static final String ATTRIBUTE_NAME_ONE = "word";
    public static final String ATTRIBUTE_NAME_TWO = "count";
    public static final String ANALYZER_TYPE = "standard";

    public static final Attribute ATTRIBUTE_ONE = new Attribute(ATTRIBUTE_NAME_ONE, AttributeType.TEXT);
    public static final Attribute ATTRIBUTE_TWO = new Attribute(ATTRIBUTE_NAME_TWO, AttributeType.INTEGER);
    public static final Attribute RESULT_ATTRIBUTE_ONE = new Attribute(ATTRIBUTE_NAME_ONE, AttributeType.STRING);
    public static final Schema WORD_CLOUD_SCHEMA = new Schema(ATTRIBUTE_ONE, ATTRIBUTE_TWO);
    public static final Schema WORD_CLOUD_RESULT_SCHEMA = new Schema(RESULT_ATTRIBUTE_ONE, ATTRIBUTE_TWO);

    public static List<Tuple> getTuples() {
        IField[] fields1 = { new TextField("foo foo"), new IntegerField(2000) };
        IField[] fields2 = { new TextField("foo foo"), new IntegerField(1200) };
        IField[] fields3 = { new TextField("amy amy"), new IntegerField(1000) };
        IField[] fields4 = { new TextField("bob the a in into this"), new IntegerField(500) };


        Tuple tuple1 = new Tuple(WORD_CLOUD_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(WORD_CLOUD_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(WORD_CLOUD_SCHEMA, fields3);
        Tuple tuple4 = new Tuple(WORD_CLOUD_SCHEMA, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }




    public static List<Tuple> getResultTuples() {
        IField[] fields1 = { new StringField("foo"), new IntegerField(200) };
        IField[] fields2 = { new StringField("amy"), new IntegerField(100) };
        IField[] fields3 = { new StringField("bob"), new IntegerField(50) };


        Tuple tuple1 = new Tuple(WORD_CLOUD_RESULT_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(WORD_CLOUD_RESULT_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(WORD_CLOUD_RESULT_SCHEMA, fields3);

        return Arrays.asList(tuple1, tuple2, tuple3);

    }

}
