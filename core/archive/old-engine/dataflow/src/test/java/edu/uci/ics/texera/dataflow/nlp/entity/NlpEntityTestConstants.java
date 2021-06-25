package edu.uci.ics.texera.dataflow.nlp.entity;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.*;

/**
 * Created by Sam on 16/4/27.
 */
public class NlpEntityTestConstants {

    public static final String SENTENCE_ONE = "sentence_one";
    public static final String SENTENCE_TWO = "sentence_two";

    public static final Attribute SENTENCE_ONE_ATTR = new Attribute(SENTENCE_ONE, AttributeType.TEXT);

    public static final Attribute SENTENCE_TWO_ATTR = new Attribute(SENTENCE_TWO, AttributeType.TEXT);

    public static final List<Attribute> ATTRIBUTES_ONE_SENTENCE = Arrays.asList(SENTENCE_ONE_ATTR);

    public static final List<Attribute> ATTRIBUTES_TWO_SENTENCE = Arrays.asList(SENTENCE_ONE_ATTR, SENTENCE_ONE_ATTR);

    public static final Schema SCHEMA_ONE_SENTENCE = new Schema(SENTENCE_ONE_ATTR);
    public static final Schema SCHEMA_TWO_SENTENCE = new Schema(SENTENCE_ONE_ATTR, SENTENCE_TWO_ATTR);
    
    public static final String RESULT = "nlp entity";
    public static final Attribute REULST_ATTRIBUTE = new Attribute(RESULT, AttributeType.LIST);
    
    public static List<Tuple> getOneSentenceTestTuple() {
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        IField[] fields2 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        IField[] fields3 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        IField[] fields4 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        IField[] fields5 = { new TextField("This backpack costs me 300 dollars.")};
        IField[] fields6 = { new TextField("What't the brand, Samsung or Apple?")};
        
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_ONE_SENTENCE, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_ONE_SENTENCE, fields3);
        Tuple tuple4 = new Tuple(SCHEMA_ONE_SENTENCE, fields4);
        Tuple tuple5 = new Tuple(SCHEMA_ONE_SENTENCE, fields5);
        Tuple tuple6 = new Tuple(SCHEMA_ONE_SENTENCE, fields6);
        
        return Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
    }
    
    public static List<Tuple> getTwoSentenceTestTuple() {
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        IField[] fields2 = { new TextField("I made an appointment at 8 am."), 
                new TextField("Aug 16, 2016 is a really important date.")};
        IField[] fields3 = { new TextField("I really love Kelly Clarkson's Because of You."),
                new TextField("Shirley Temple is a very famous actress.")};
        
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_TWO_SENTENCE, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_TWO_SENTENCE, fields3);
        
        return Arrays.asList(tuple1, tuple2, tuple3);
    }

    public static List<Tuple> getTest1Tuple() throws ParseException {
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<Tuple> getTest2Tuple() throws ParseException {

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<Tuple> getTest3Tuple() throws ParseException {

        IField[] fields1 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<Tuple> getTest4Tuple() throws ParseException {

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<Tuple> getTest7Tuple() throws ParseException {
        IField[] fields1 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getTest8Tuple() {
    	IField[] fields1 = { new TextField("This backpack costs me 300 dollars.")};
    	Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
    	return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getTest9Tuple() {
    	IField[] fields1 = {new TextField("I made an appointment at 8 am."), new TextField("Aug 16, 2016 is a really important date.")};
    	Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
    	return Arrays.asList(tuple1);
    }

    public static List<Tuple> getTest1ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        spanList.add(span1);

        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();

        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest2ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");
        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest3ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");
        Span span4 = new Span("sentence_one", 53, 65, NlpEntityType.PERSON.toString(), "Donald Trump");
        Span span5 = new Span("sentence_one", 70, 82, NlpEntityType.PERSON.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest4ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");
        Span span4 = new Span("sentence_two", 0, 12, NlpEntityType.PERSON.toString(), "Donald Trump");
        Span span5 = new Span("sentence_two", 17, 29, NlpEntityType.PERSON.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest5ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_two", 0, 12, NlpEntityType.PERSON.toString(), "Donald Trump");
        Span span2 = new Span("sentence_two", 17, 29, NlpEntityType.PERSON.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest6ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }

    public static List<Tuple> getTest7ResultTuples() {
        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 12, 16, NlpEntityType.ADJECTIVE.toString(), "warm");
        spanList.add(span1);

        IField[] fields1 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }
    
    public static List<Tuple> getTest8ResultTuples() {
    	    List<Span> spanList = new ArrayList<Span>();
    	
    	    Span span1 = new Span("sentence_one", 23, 34, NlpEntityType.MONEY.toString(), "300 dollars");
    	    spanList.add(span1);
    			
        IField[] fields1 = {new TextField("This backpack costs me 300 dollars.")};
    	    Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);

        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();
        return Arrays.asList(returnTuple);
    }
    
    public static List<Tuple> getTest9ResultTuples() {
        	List<Span> spanList = new ArrayList<Span>();
        	
        	Span span1 = new Span("sentence_one", 25, 29, NlpEntityType.TIME.toString(), "8 am");
        	Span span2 = new Span("sentence_two", 0, 12, NlpEntityType.DATE.toString(), "Aug 16 , 2016");
        	
        	spanList.add(span1);
        	spanList.add(span2);
        	IField[] fields1 = {new TextField("I made an appointment at 8 am."), new TextField("Aug 16, 2016 is a really important date.")};
        	Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        	
        Tuple returnTuple = new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build();

        	return Arrays.asList(returnTuple);
    }
    
    public static List<Tuple> getTest10ResultTuples(){
        List<Tuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");
        Span span4 = new Span("sentence_one", 53, 65, NlpEntityType.PERSON.toString(), "Donald Trump");
        Span span5 = new Span("sentence_one", 70, 82, NlpEntityType.PERSON.toString(), "Barack Obama");
        Span span6 = new Span("sentence_one", 23, 34, NlpEntityType.MONEY.toString(), "300 dollars");
        Span span7 = new Span("sentence_one", 18, 25, NlpEntityType.ORGANIZATION.toString(), "Samsung");
        
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        IField[] fields2 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        IField[] fields3 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        IField[] fields5 = { new TextField("This backpack costs me 300 dollars.")};
        IField[] fields6 = { new TextField("What't the brand, Samsung or Apple?")};
        
        Tuple tuple1 = new Tuple(SCHEMA_ONE_SENTENCE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_ONE_SENTENCE, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_ONE_SENTENCE, fields3);
        Tuple tuple5 = new Tuple(SCHEMA_ONE_SENTENCE, fields5);
        Tuple tuple6 = new Tuple(SCHEMA_ONE_SENTENCE, fields6);
                
        spanList.add(span1);
        resultList.add(new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.add(span2);
        spanList.add(span3);
        resultList.add(new Tuple.Builder(tuple2).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.add(span4);
        spanList.add(span5);
        resultList.add(new Tuple.Builder(tuple3).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.clear();
        spanList.add(span6);
        resultList.add(new Tuple.Builder(tuple5).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.clear();
        spanList.add(span7);
        resultList.add( new Tuple.Builder(tuple6).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        return resultList;
    }
    
    public static List<Tuple> getTest11ResultTuple() {
        List<Tuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span("sentence_one", 0, 9, NlpEntityType.ORGANIZATION.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpEntityType.ORGANIZATION.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpEntityType.ORGANIZATION.toString(), "Facebook");
        Span span4 = new Span("sentence_two", 0, 12, NlpEntityType.PERSON.toString(), "Donald Trump");
        Span span5 = new Span("sentence_two", 17, 29, NlpEntityType.PERSON.toString(), "Barack Obama");
        Span span6 = new Span("sentence_one", 25 ,29, NlpEntityType.TIME.toString(), "8 am");
        Span span7 = new Span("sentence_two", 0, 12, NlpEntityType.DATE.toString(), "Aug 16 , 2016");
        Span span8 = new Span("sentence_one", 14, 28, NlpEntityType.PERSON.toString(), "Kelly Clarkson");
        Span span9 = new Span("sentence_two", 0, 14, NlpEntityType.PERSON.toString(), "Shirley Temple");
        
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        IField[] fields2 = { new TextField("I made an appointment at 8 am."), 
                new TextField("Aug 16, 2016 is a really important date.")};
        IField[] fields3 = { new TextField("I really love Kelly Clarkson's Because of You."),
                new TextField("Shirley Temple is a very famous actress.")};
        
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_TWO_SENTENCE, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_TWO_SENTENCE, fields3);
                
        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);
        resultList.add(new Tuple.Builder(tuple1).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.clear();
        spanList.add(span6);
        spanList.add(span7);
        resultList.add(new Tuple.Builder(tuple2).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        spanList.clear();
        spanList.add(span8);
        spanList.add(span9);
        resultList.add(new Tuple.Builder(tuple3).add(REULST_ATTRIBUTE, new ListField<Span>(spanList)).build());
        
        return resultList;
    }
}
