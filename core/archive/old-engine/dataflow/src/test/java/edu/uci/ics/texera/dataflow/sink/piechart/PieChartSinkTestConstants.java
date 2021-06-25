package edu.uci.ics.texera.dataflow.sink.piechart;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.Arrays;
import java.util.List;

public class PieChartSinkTestConstants {
    public static final String ATTRIBUTE_NAME_ONE = "id";
    public static final String ATTRIBUTE_NAME_TWO = "name";
    public static final String ATTRIBUTE_NAME_THREE = "number_of_followers";
    public static final String ATTRIBUTE_NAME_FOUR = "gender";

    public static final Attribute ATTRIBUTE_ONE = new Attribute(ATTRIBUTE_NAME_ONE, AttributeType.INTEGER);
    public static final Attribute ATTRIBUTE_TWO = new Attribute(ATTRIBUTE_NAME_TWO, AttributeType.STRING);
    public static final Attribute ATTRIBUTE_THREE = new Attribute(ATTRIBUTE_NAME_THREE, AttributeType.INTEGER);
    public static final Attribute ATTRIBUTE_FOUR = new Attribute(ATTRIBUTE_NAME_FOUR, AttributeType.STRING);
    public static final Attribute ATTRIBUTE_FIVE = new Attribute(ATTRIBUTE_NAME_THREE, AttributeType.DOUBLE);


    public static final Schema PIE_SCHEMA = new Schema(ATTRIBUTE_ONE, ATTRIBUTE_TWO, ATTRIBUTE_THREE, ATTRIBUTE_FOUR);
    public static final Schema PIE_RESULT_SCHEMA = new Schema(ATTRIBUTE_TWO, ATTRIBUTE_FIVE);

    public static List<Tuple> getTuples() {
        IField[] fields1 = { new IntegerField(1), new StringField("Tom"), new IntegerField(10000), new StringField("M") };
        IField[] fields2 = { new IntegerField(2), new StringField("Jerry"), new IntegerField(80), new StringField("M") };
        IField[] fields3 = { new IntegerField(3), new StringField("Trump"), new IntegerField(80), new StringField("M") };
        IField[] fields4 = { new IntegerField(4), new StringField("Bob"), new IntegerField(70), new StringField("M") };


        Tuple tuple1 = new Tuple(PIE_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(PIE_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(PIE_SCHEMA, fields3);
        Tuple tuple4 = new Tuple(PIE_SCHEMA, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }


    public static List<Tuple> getResultTuples() {
        IField[] fields1 = { new StringField("Tom"), new IntegerField(1000) };
        IField[] fields2 = { new StringField("Other"), new IntegerField(230) };

        Tuple tuple1 = new Tuple(PIE_RESULT_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(PIE_RESULT_SCHEMA, fields2);


        return Arrays.asList(tuple1, tuple2);
    }

}
