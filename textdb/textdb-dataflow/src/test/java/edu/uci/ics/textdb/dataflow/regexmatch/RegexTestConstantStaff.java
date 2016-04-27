package edu.uci.ics.textdb.dataflow.regexmatch;

import java.text.ParseException;
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
 * Test Data Staff 
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
    
    public static final List<Attribute> ATTRIBUTES_STAFF = Arrays.asList(
    		FIRST_NAME_ATTR, LAST_NAME_ATTR, EMAIL_ATTR, PHONE_ATTR);
    public static final Schema SCHEMA_STAFF = new Schema(ATTRIBUTES_STAFF);
    
    public static List<ITuple> getSampleStaffTuples() throws ParseException {
    	IField[] fields1 = {new StringField("Karina"), new StringField("Bocanegra"), new StringField("k.bocanegra@uci.edu"), new StringField("(949) 824-5156")};
    	IField[] fields2 = {new StringField("Lumen"), new StringField("Hwang"), new StringField("hwangl@ics.uci.edu"), new StringField("(949) 824-8088")};
    	IField[] fields3 = {new StringField("Jessica"), new StringField("Shanahan "), new StringField("shanahan@uci.edu"), new StringField("(949) 824-7550")};
    	IField[] fields4 = {new StringField("Mare"), new StringField("Stasik"), new StringField("mstasik@ics.uci.edu"), new StringField("(949) 824-7047")};
    	
    	ITuple tuple1 = new DataTuple(SCHEMA_STAFF, fields1);
    	ITuple tuple2 = new DataTuple(SCHEMA_STAFF, fields2);
    	ITuple tuple3 = new DataTuple(SCHEMA_STAFF, fields3);
    	ITuple tuple4 = new DataTuple(SCHEMA_STAFF, fields4);

    	return Arrays.asList(tuple1, tuple2, tuple3, tuple4);
    }
}
