package edu.uci.ics.texera.api.constants.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.*;

/**
 * @author Qinhua Huang
 *         Chinese data constants, used by Chinese unit tests.
 */
public class TestConstantsChinese {
    // Sample Fields
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String AGE = "age";
    public static final String HEIGHT = "height";
    public static final String DATE_OF_BIRTH = "dateOfBirth";
    public static final String DESCRIPTION = "description";

    public static final Attribute FIRST_NAME_ATTR = new Attribute(FIRST_NAME, AttributeType.STRING);
    public static final Attribute LAST_NAME_ATTR = new Attribute(LAST_NAME, AttributeType.STRING);
    public static final Attribute AGE_ATTR = new Attribute(AGE, AttributeType.INTEGER);
    public static final Attribute HEIGHT_ATTR = new Attribute(HEIGHT, AttributeType.DOUBLE);
    public static final Attribute DATE_OF_BIRTH_ATTR = new Attribute(DATE_OF_BIRTH, AttributeType.DATE);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, AttributeType.TEXT);

    // Sample Schema
    public static final Attribute[] ATTRIBUTES_PEOPLE = { FIRST_NAME_ATTR, LAST_NAME_ATTR, AGE_ATTR, HEIGHT_ATTR,
            DATE_OF_BIRTH_ATTR, DESCRIPTION_ATTR };
    public static final Schema SCHEMA_PEOPLE = new Schema(ATTRIBUTES_PEOPLE);

    public static List<Tuple> getSamplePeopleTuples() {
        
        try {
            IField[] fields1 = { new StringField("无忌"), new StringField("长孙"), new IntegerField(46),
                    new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                    new TextField("北京大学电气工程学院") };
            IField[] fields2 = { new StringField("孔明"), new StringField("洛克贝尔"),
                    new IntegerField(42), new DoubleField(5.99),
                    new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("北京大学计算机学院") };
            IField[] fields3 = { new StringField("宋江"), new StringField("建筑"),
                    new IntegerField(42), new DoubleField(5.99),
                    new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), 
                    new TextField("伟大的建筑是历史的坐标，具有传承的价值。") };
           
            Tuple tuple1 = new Tuple(SCHEMA_PEOPLE, fields1);
            Tuple tuple2 = new Tuple(SCHEMA_PEOPLE, fields2);
            Tuple tuple3 = new Tuple(SCHEMA_PEOPLE, fields3);
            
            return Arrays.asList(tuple1, tuple2, tuple3);
        } catch (ParseException e) {
            // exception should not happen because we know the data is correct
            e.printStackTrace();
            return Arrays.asList();
        }

    }
}
