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
 *         Test Data Staff
 */
public class RegexTestConstantStaff {
    // Sample test data about Staff
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String EMAIL = "email";
    public static final String PHONE = "phone";

    public static final Attribute FIRST_NAME_ATTR = new Attribute(FIRST_NAME, AttributeType.STRING);
    public static final Attribute LAST_NAME_ATTR = new Attribute(LAST_NAME, AttributeType.STRING);
    public static final Attribute EMAIL_ATTR = new Attribute(EMAIL, AttributeType.STRING);
    public static final Attribute PHONE_ATTR = new Attribute(PHONE, AttributeType.STRING);

    public static final Attribute[] ATTRIBUTES_STAFF = { FIRST_NAME_ATTR, LAST_NAME_ATTR, EMAIL_ATTR, PHONE_ATTR };
    public static final Schema SCHEMA_STAFF = new Schema(ATTRIBUTES_STAFF);

    public static List<Tuple> getSampleStaffTuples() {
        IField[] fields1 = { new StringField("Melody"), new StringField("Bocanegra"),
                new StringField("m.bocanegra@164.com"), new StringField("(945) 734-5156") };
        IField[] fields2 = { new StringField("Kanon"), new StringField("Hwang"), new StringField("hwangk@ske.akb.edu"),
                new StringField("(494) 352-8098") };
        IField[] fields3 = { new StringField("Shirley"), new StringField("Clarkson"),
                new StringField("clarkson@facebook"), new StringField("(587) 241-7550") };
        IField[] fields4 = { new StringField("Lucy"), new StringField("Kimoto"),
                new StringField("lki?moto@microsoft.com"), new StringField("(499) 824-3625") };

        Tuple tuple1 = new Tuple(SCHEMA_STAFF, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_STAFF, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_STAFF, fields3);
        Tuple tuple4 = new Tuple(SCHEMA_STAFF, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }
}
