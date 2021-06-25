package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.*;

/**
 * @author laisycs
 * @author zuozhi
 *
 *         Test Data Corporations
 */
public class RegexTestConstantsCorp {
    // Sample test data about Corporations
    public static final String CORP_NAME = "name";
    public static final String URL = "url";
    public static final String IP_ADDRESS = "ip";

    public static final Attribute CORP_NAME_ATTR = new Attribute(CORP_NAME, AttributeType.STRING);
    public static final Attribute URL_ATTR = new Attribute(URL, AttributeType.STRING);
    public static final Attribute IP_ADDRESS_ATTR = new Attribute(IP_ADDRESS, AttributeType.STRING);

    public static final Attribute[] ATTRIBUTES_CORP = { CORP_NAME_ATTR, URL_ATTR, IP_ADDRESS_ATTR };
    public static final Schema SCHEMA_CORP = new Schema(ATTRIBUTES_CORP);

    public static List<Tuple> getSampleCorpTuples() {
        IField[] fields1 = { new StringField("Facebook"), new StringField("404 Not Found"),
                new StringField("66.220.144.0") };
        IField[] fields2 = { new StringField("Weibo"), new StringField("http://weibo.com"),
                new StringField("180.149.134.141") };
        IField[] fields3 = { new StringField("Microsoft"), new StringField("https://www.microsoft.com/en-us/"),
                new StringField("131.107.0.89") };
        IField[] fields4 = { new StringField("Google"), new StringField("websit: www.google.com"),
                new StringField("8.8.8.8.8.8") };

        Tuple tuple1 = new Tuple(SCHEMA_CORP, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_CORP, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_CORP, fields3);
        Tuple tuple4 = new Tuple(SCHEMA_CORP, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }
}
