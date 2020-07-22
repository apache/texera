package edu.uci.ics.texera.dataflow.sink.wordcloud;

import static org.junit.Assert.assertEquals;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.Arrays;
import java.util.List;

public class WordCloudSinkTestConstants {
    public static final String ATTRIBUTE_NAME_ONE = "word";
    public static final String ATTRIBUTE_NAME_TWO = "count";

    public static final Attribute ATTRIBUTE_ONE = new Attribute(ATTRIBUTE_NAME_ONE, AttributeType.STRING);
    public static final Attribute ATTRIBUTE_TWO = new Attribute(ATTRIBUTE_NAME_TWO, AttributeType.INTEGER);

    public static final Schema WORD_CLOUD_SCHEMA = new Schema(ATTRIBUTE_ONE, ATTRIBUTE_TWO);
    public static final Schema INVALID_COLUMN_NAME_WORD_CLOUD_SCHEMA = new Schema(new Attribute("w", AttributeType.STRING), new Attribute("c", AttributeType.INTEGER));
    public static final Schema INVALID_DATA_TYPE_WORD_CLOUD_SCHEMA_TWO =  new Schema(new Attribute("word", AttributeType.STRING), new Attribute("count", AttributeType.STRING));
    public static List<Tuple> getTuples() {
        IField[] fields1 = { new StringField("foo"), new IntegerField(2000) };
        IField[] fields2 = { new StringField("bar"), new IntegerField(1200) };
        IField[] fields3 = { new StringField("amy"), new IntegerField(1000) };
        IField[] fields4 = { new StringField("bob"), new IntegerField(500) };


        Tuple tuple1 = new Tuple(WORD_CLOUD_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(WORD_CLOUD_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(WORD_CLOUD_SCHEMA, fields3);
        Tuple tuple4 = new Tuple(WORD_CLOUD_SCHEMA, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }

    public static List<Tuple> getTuplesWithInvaildColumnName() {
        IField[] fields1 = { new StringField("foo"), new IntegerField(2000) };
        IField[] fields2 = { new StringField("bar"), new IntegerField(1200) };
        IField[] fields3 = { new StringField("amy"), new IntegerField(1000) };
        IField[] fields4 = { new StringField("bob"), new IntegerField(500) };


        Tuple tuple1 = new Tuple(INVALID_COLUMN_NAME_WORD_CLOUD_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(INVALID_COLUMN_NAME_WORD_CLOUD_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(INVALID_COLUMN_NAME_WORD_CLOUD_SCHEMA, fields3);
        Tuple tuple4 = new Tuple(INVALID_COLUMN_NAME_WORD_CLOUD_SCHEMA, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }

    public static List<Tuple> getTuplesWithInvaildDataType() {
        IField[] fields1 = { new StringField("foo"),  new StringField("foo") };
        IField[] fields2 = { new StringField("bar"),  new StringField("foo")};
        IField[] fields3 = { new StringField("amy"),  new StringField("foo") };
        IField[] fields4 = { new StringField("bob"),  new StringField("foo") };


        Tuple tuple1 = new Tuple(INVALID_DATA_TYPE_WORD_CLOUD_SCHEMA_TWO, fields1);
        Tuple tuple2 = new Tuple(INVALID_DATA_TYPE_WORD_CLOUD_SCHEMA_TWO, fields2);
        Tuple tuple3 = new Tuple(INVALID_DATA_TYPE_WORD_CLOUD_SCHEMA_TWO, fields3);
        Tuple tuple4 = new Tuple(INVALID_DATA_TYPE_WORD_CLOUD_SCHEMA_TWO, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }

    public static List<Tuple> getResultTuples() {
        IField[] fields1 = { new StringField("foo"), new IntegerField(200) };
        IField[] fields2 = { new StringField("bar"), new IntegerField(120) };
        IField[] fields3 = { new StringField("amy"), new IntegerField(100) };
        IField[] fields4 = { new StringField("bob"), new IntegerField(50) };


        Tuple tuple1 = new Tuple(WORD_CLOUD_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(WORD_CLOUD_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(WORD_CLOUD_SCHEMA, fields3);
        Tuple tuple4 = new Tuple(WORD_CLOUD_SCHEMA, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);

    }

}
