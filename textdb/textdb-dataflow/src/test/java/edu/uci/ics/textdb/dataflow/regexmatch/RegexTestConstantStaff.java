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
 *         Test Data Staff
 */
public class RegexTestConstantStaff {
    // Sample test data about Staff
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String EMAIL = "email";
    public static final String PHONE = "phone";

    public static final Attribute FIRST_NAME_ATTR = new Attribute(FIRST_NAME, FieldType.STRING);
    public static final Attribute LAST_NAME_ATTR = new Attribute(LAST_NAME, FieldType.STRING);
    public static final Attribute EMAIL_ATTR = new Attribute(EMAIL, FieldType.STRING);
    public static final Attribute PHONE_ATTR = new Attribute(PHONE, FieldType.STRING);

    public static final Attribute[] ATTRIBUTES_STAFF = { FIRST_NAME_ATTR, LAST_NAME_ATTR, EMAIL_ATTR, PHONE_ATTR };
    public static final Schema SCHEMA_STAFF = new Schema(ATTRIBUTES_STAFF);

    public static List<ITuple> getSampleStaffTuples() {
        IField[] fields1 = { new StringField("Melody"), new StringField("Bocanegra"),
                new StringField("m.bocanegra@164.com"), new StringField("(945) 734-5156") };
        IField[] fields2 = { new StringField("Kanon"), new StringField("Hwang"), new StringField("hwangk@ske.akb.edu"),
                new StringField("(494) 352-8098") };
        IField[] fields3 = { new StringField("Shirley"), new StringField("Clarkson"),
                new StringField("clarkson@facebook"), new StringField("(587) 241-7550") };
        IField[] fields4 = { new StringField("Lucy"), new StringField("Kimoto"),
                new StringField("lki?moto@microsoft.com"), new StringField("(499) 824-3625") };

        ITuple tuple1 = new DataTuple(SCHEMA_STAFF, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_STAFF, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_STAFF, fields3);
        ITuple tuple4 = new DataTuple(SCHEMA_STAFF, fields4);

        return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }
}
