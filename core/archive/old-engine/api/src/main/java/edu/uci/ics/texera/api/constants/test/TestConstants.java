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
 * @author sandeepreddy602 Including this class in src/main/java since it is
 *         required by other projects Keeping it in src/test/java doesn't make
 *         it available to the other projects
 */
public class TestConstants {
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
            IField[] fields0 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                    new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                    new TextField("Tall Angry") };
            IField[] fields1 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                    new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                    new TextField("Short Brown") };
            IField[] fields2 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                    new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                    new TextField("White Angry") };
            IField[] fields3 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                    new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                    new TextField("Lin Clooney is Short and lin clooney is Angry") };
            IField[] fields4 = { new StringField("christian john wayne"), new StringField("rock bale"), new IntegerField(42), 
                    new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair") };
            IField[] fields5 = { new StringField("Mary brown"), new StringField("Lake Forest"), new IntegerField(42), 
                    new DoubleField(5.99), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Short angry") };

            Tuple tuple0 = new Tuple(SCHEMA_PEOPLE, fields0);
            Tuple tuple1 = new Tuple(SCHEMA_PEOPLE, fields1);
            Tuple tuple2 = new Tuple(SCHEMA_PEOPLE, fields2);
            Tuple tuple3 = new Tuple(SCHEMA_PEOPLE, fields3);
            Tuple tuple4 = new Tuple(SCHEMA_PEOPLE, fields4);
            Tuple tuple5 = new Tuple(SCHEMA_PEOPLE, fields5);

            return Arrays.asList(tuple0, tuple1, tuple2, tuple3, tuple4, tuple5);   
        } catch (ParseException e) {
            // exception should not happen because we know the data is correct
            e.printStackTrace();
            return Arrays.asList();
        }

    }

}
