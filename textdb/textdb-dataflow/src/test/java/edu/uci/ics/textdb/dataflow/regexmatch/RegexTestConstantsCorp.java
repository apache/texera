package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;

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

    public static final Attribute CORP_NAME_ATTR = new Attribute(CORP_NAME, FieldType.STRING);
    public static final Attribute URL_ATTR = new Attribute(URL, FieldType.STRING);
    public static final Attribute IP_ADDRESS_ATTR = new Attribute(IP_ADDRESS, FieldType.STRING);

    public static final Attribute[] ATTRIBUTES_CORP = { CORP_NAME_ATTR, URL_ATTR, IP_ADDRESS_ATTR };
    public static final Schema SCHEMA_CORP = new Schema(ATTRIBUTES_CORP);

    public static List<ITuple> getSampleCorpTuples() {
        IField[] fields1 = { new StringField("Facebook"), new StringField("404 Not Found"),
                new StringField("66.220.144.0") };
        IField[] fields2 = { new StringField("Weibo"), new StringField("http://weibo.com"),
                new StringField("180.149.134.141") };
        IField[] fields3 = { new StringField("Microsoft"), new StringField("https://www.microsoft.com/en-us/"),
                new StringField("131.107.0.89") };
        IField[] fields4 = { new StringField("Google"), new StringField("websit: www.google.com"),
                new StringField("8.8.8.8.8.8") };

        ITuple tuple1 = new DataTuple(SCHEMA_CORP, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_CORP, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_CORP, fields3);
        ITuple tuple4 = new DataTuple(SCHEMA_CORP, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }
}
