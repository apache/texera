package edu.uci.ics.texera.dataflow.sink.barchart;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.Arrays;
import java.util.List;

public class BarChartSinkTestConstants {
    public static final String ATTRIBUTE_NAME_ONE = "id";
    public static final String ATTRIBUTE_NAME_TWO = "name";
    public static final String ATTRIBUTE_NAME_THREE = "grade";
    public static final String ATTRIBUTE_NAME_FOUR = "gender";

    public static final Attribute ATTRIBUTE_ONE = new Attribute(ATTRIBUTE_NAME_ONE, AttributeType.INTEGER);
    public static final Attribute ATTRIBUTE_TWO = new Attribute(ATTRIBUTE_NAME_TWO, AttributeType.STRING);
    public static final Attribute ATTRIBUTE_THREE = new Attribute(ATTRIBUTE_NAME_THREE, AttributeType.INTEGER);
    public static final Attribute ATTRIBUTE_FOUR = new Attribute(ATTRIBUTE_NAME_FOUR, AttributeType.STRING);
    public static final Schema BAR_SCHEMA = new Schema(ATTRIBUTE_ONE, ATTRIBUTE_TWO, ATTRIBUTE_THREE, ATTRIBUTE_FOUR);
    public static final Schema BAR_RESULT_SCHEMA = new Schema(ATTRIBUTE_TWO, ATTRIBUTE_THREE);

    public static List<Tuple> getTuples() {
        IField[] fields1 = { new IntegerField(1), new StringField("Tom"), new IntegerField(90), new StringField("M") };
        IField[] fields2 = { new IntegerField(2), new StringField("Jerry"), new IntegerField(80), new StringField("M") };
        IField[] fields3 = { new IntegerField(3), new StringField("Bob"), new IntegerField(70), new StringField("M") };


        Tuple tuple1 = new Tuple(BAR_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(BAR_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(BAR_SCHEMA, fields3);

        return Arrays.asList(tuple1, tuple2, tuple3);
    }


    public static List<Tuple> getResultTuples() {
        IField[] fields1 = { new StringField("Tom"), new IntegerField(90) };
        IField[] fields2 = { new StringField("Jerry"), new IntegerField(80) };
        IField[] fields3 = { new StringField("Bob"), new IntegerField(70) };


        Tuple tuple1 = new Tuple(BAR_RESULT_SCHEMA, fields1);
        Tuple tuple2 = new Tuple(BAR_RESULT_SCHEMA, fields2);
        Tuple tuple3 = new Tuple(BAR_RESULT_SCHEMA, fields3);

        return Arrays.asList(tuple1, tuple2, tuple3);
    }
}
