package edu.uci.ics.texera.api.constants.test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.DateTimeField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;


/**
 * 
 * @author Qinhua Huang
 *
 */
public class TestConstantsRegexSplit {
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
    public static final Attribute DATE_OF_BIRTH_ATTR = new Attribute(DATE_OF_BIRTH, AttributeType.DATETIME);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, AttributeType.TEXT);

    // Sample Schema
    public static final Attribute[] ATTRIBUTES_PEOPLE = { FIRST_NAME_ATTR, LAST_NAME_ATTR, AGE_ATTR, HEIGHT_ATTR,
            DATE_OF_BIRTH_ATTR, DESCRIPTION_ATTR };
    public static final Schema SCHEMA_PEOPLE = new Schema(ATTRIBUTES_PEOPLE);

    public static List<Tuple> constructSamplePeopleTuples() {
        
        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateTimeField(LocalDateTime.parse("1970-01-01T11:11:11")),
                new TextField("banana") };
        IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                new DoubleField(5.95), new DateTimeField(LocalDateTime.parse("1980-01-02T13:14:15")),
                new TextField("mississippi") };
        
        Tuple tuple1 = new Tuple(SCHEMA_PEOPLE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_PEOPLE, fields2);
        
        return Arrays.asList(tuple1, tuple2);   

    }
}
