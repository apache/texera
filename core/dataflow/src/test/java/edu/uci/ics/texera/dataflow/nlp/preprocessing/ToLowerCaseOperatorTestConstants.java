package edu.uci.ics.texera.dataflow.nlp.preprocessing;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ToLowerCaseOperatorTestConstants {
    public static final Attribute ID_ATTRIBUTE = new Attribute("id", AttributeType.INTEGER);
    public static final Attribute STRING_ATTRIBUTE = new Attribute("tweet", AttributeType.STRING);
    public static final Attribute RESULT_ATTRIBUTE = new Attribute("result", AttributeType.STRING);
    public static final Schema SCHMEA = new Schema(ID_ATTRIBUTE, STRING_ATTRIBUTE);
    public static final Schema RESULT_SCHEMA = new Schema(ID_ATTRIBUTE, STRING_ATTRIBUTE, RESULT_ATTRIBUTE);
    public static final String testString = "THIS IS UPPER";
    public static final String testResultString = "this is upper";

    public static List<Tuple> getTestTuple() {
        IField field[] = { new IntegerField(1), new StringField(testString)};
        Tuple tuple = new Tuple(SCHMEA, field);
        return Arrays.asList(tuple);
    }

    public static List<Tuple> getTestResultTuple() {
        IField field[] = { new IntegerField(1),  new StringField(testString), new StringField(testResultString)};

        Tuple tuple = new Tuple(RESULT_SCHEMA, field);
        return Arrays.asList(tuple);

    }
}
