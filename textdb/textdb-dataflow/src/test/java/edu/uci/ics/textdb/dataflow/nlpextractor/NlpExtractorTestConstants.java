package edu.uci.ics.textdb.dataflow.nlpextractor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;

/**
 * Created by Sam on 16/4/27.
 */
public class NlpExtractorTestConstants {

    public static final String SENTENCE_ONE = "sentence_one";
    public static final String SENTENCE_TWO = "sentence_two";

    public static final Attribute SENTENCE_ONE_ATTR = new Attribute(SENTENCE_ONE, FieldType.TEXT);

    public static final Attribute SENTENCE_TWO_ATTR = new Attribute(SENTENCE_TWO, FieldType.TEXT);

    public static final List<Attribute> ATTRIBUTES_ONE_SENTENCE = Arrays.asList(SENTENCE_ONE_ATTR);

    public static final List<Attribute> ATTRIBUTES_TWO_SENTENCE = Arrays.asList(SENTENCE_ONE_ATTR, SENTENCE_ONE_ATTR);

    public static final Schema SCHEMA_ONE_SENTENCE = new Schema(SENTENCE_ONE_ATTR);
    public static final Schema SCHEMA_TWO_SENTENCE = new Schema(SENTENCE_ONE_ATTR, SENTENCE_TWO_ATTR);
    
    public static List<ITuple> getOneSentenceTestTuple() {
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        IField[] fields2 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        IField[] fields3 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        IField[] fields4 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        IField[] fields5 = { new TextField("This backpack costs me 300 dollars.")};
        IField[] fields6 = { new TextField("What't the brand, Samsung or Apple?")};
        
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_ONE_SENTENCE, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_ONE_SENTENCE, fields3);
        ITuple tuple4 = new DataTuple(SCHEMA_ONE_SENTENCE, fields4);
        ITuple tuple5 = new DataTuple(SCHEMA_ONE_SENTENCE, fields5);
        ITuple tuple6 = new DataTuple(SCHEMA_ONE_SENTENCE, fields6);
        
        return Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
    }
    
    public static List<ITuple> getTwoSentenceTestTuple() {
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        IField[] fields2 = { new TextField("I made an appointment at 8 am."), 
                new TextField("Aug 16, 2016 is a really important date.")};
        IField[] fields3 = { new TextField("I really love Kelly Clarkson's Because of You."),
                new TextField("Shirley Temple is a very famous actress.")};
        
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_TWO_SENTENCE, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_TWO_SENTENCE, fields3);
        
        return Arrays.asList(tuple1, tuple2, tuple3);
    }

    public static List<ITuple> getTest1Tuple() throws ParseException {
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest2Tuple() throws ParseException {

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest3Tuple() throws ParseException {

        IField[] fields1 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest4Tuple() throws ParseException {

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest7Tuple() throws ParseException {
        IField[] fields1 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<ITuple> getTest8Tuple() {
    	IField[] fields1 = { new TextField("This backpack costs me 300 dollars.")};
    	ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
    	return Arrays.asList(tuple1);
    }
    
    public static List<ITuple> getTest9Tuple() {
    	IField[] fields1 = {new TextField("I made an appointment at 8 am."), new TextField("Aug 16, 2016 is a really important date.")};
    	ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
    	return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest1ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        spanList.add(span1);

        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest2ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");
        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);
        return resultList;
    }

    public static List<ITuple> getTest3ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_one", 53, 65, NlpPredicate.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_one", 70, 82, NlpPredicate.NlpTokenType.Person.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest4ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_two", 0, 12, NlpPredicate.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_two", 17, 29, NlpPredicate.NlpTokenType.Person.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest5ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_two", 0, 12, NlpPredicate.NlpTokenType.Person.toString(), "Donald Trump");
        Span span2 = new Span("sentence_two", 17, 29, NlpPredicate.NlpTokenType.Person.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest6ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);

        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest7ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 12, 16, NlpPredicate.NlpTokenType.Adjective.toString(), "warm");
        spanList.add(span1);

        IField[] fields1 = { new TextField(
                "Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.") };
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
    
    public static List<ITuple> getTest8ResultTuples() {
    	List<ITuple> resultList = new ArrayList<>();
    	List<Span> spanList = new ArrayList<Span>();
    	
    	Span span1 = new Span("sentence_one", 23, 34, NlpPredicate.NlpTokenType.Money.toString(), "300 dollars");
    	spanList.add(span1);
    			
        IField[] fields1 = {new TextField("This backpack costs me 300 dollars.")};
    	ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
    
    public static List<ITuple> getTest9ResultTuples() {
    	List<ITuple> resultList = new ArrayList<>();
    	List<Span> spanList = new ArrayList<Span>();
    	
    	Span span1 = new Span("sentence_one", 25, 29, NlpPredicate.NlpTokenType.Time.toString(), "8 am");
    	Span span2 = new Span("sentence_two", 0, 12, NlpPredicate.NlpTokenType.Date.toString(), "Aug 16 , 2016");
    	
    	spanList.add(span1);
    	spanList.add(span2);
    	IField[] fields1 = {new TextField("I made an appointment at 8 am."), new TextField("Aug 16, 2016 is a really important date.")};
    	ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
    	
    	Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());
    	
    	ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
    	resultList.add(returnTuple);
    	
    	return resultList;
    }
    
    public static List<ITuple> getTest10ResultTuples(){
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_one", 53, 65, NlpPredicate.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_one", 70, 82, NlpPredicate.NlpTokenType.Person.toString(), "Barack Obama");
        Span span6 = new Span("sentence_one", 23, 34, NlpPredicate.NlpTokenType.Money.toString(), "300 dollars");
        Span span7 = new Span("sentence_one", 18, 25, NlpPredicate.NlpTokenType.Organization.toString(), "Samsung");
        
        IField[] fields1 = { new TextField("Microsoft is an organization.") };
        IField[] fields2 = { new TextField("Microsoft, Google and Facebook are organizations.") };
        IField[] fields3 = { new TextField(
                "Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.") };
        IField[] fields5 = { new TextField("This backpack costs me 300 dollars.")};
        IField[] fields6 = { new TextField("What't the brand, Samsung or Apple?")};
        
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_ONE_SENTENCE, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_ONE_SENTENCE, fields3);
        ITuple tuple5 = new DataTuple(SCHEMA_ONE_SENTENCE, fields5);
        ITuple tuple6 = new DataTuple(SCHEMA_ONE_SENTENCE, fields6);
        
        Schema returnSchema1 = Utils.createSpanSchema(tuple1.getSchema());
        Schema returnSchema2 = Utils.createSpanSchema(tuple2.getSchema());
        Schema returnSchema3 = Utils.createSpanSchema(tuple3.getSchema());
        Schema returnSchema5 = Utils.createSpanSchema(tuple5.getSchema());
        Schema returnSchema6 = Utils.createSpanSchema(tuple6.getSchema());
        
        spanList.add(span1);
        resultList.add(Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema1));
        
        spanList.add(span2);
        spanList.add(span3);
        resultList.add(Utils.getSpanTuple(tuple2.getFields(), spanList, returnSchema2));
        
        spanList.add(span4);
        spanList.add(span5);
        resultList.add(Utils.getSpanTuple(tuple3.getFields(), spanList, returnSchema3));
        
        spanList.clear();
        spanList.add(span6);
        resultList.add(Utils.getSpanTuple(tuple5.getFields(), spanList, returnSchema5));
        
        spanList.clear();
        spanList.add(span7);
        resultList.add(Utils.getSpanTuple(tuple6.getFields(), spanList, returnSchema6));
        
        return resultList;
    }
    
    public static List<ITuple> getTest11ResultTuple() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span("sentence_one", 0, 9, NlpPredicate.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpPredicate.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpPredicate.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_two", 0, 12, NlpPredicate.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_two", 17, 29, NlpPredicate.NlpTokenType.Person.toString(), "Barack Obama");
        Span span6 = new Span("sentence_one", 25 ,29, NlpPredicate.NlpTokenType.Time.toString(), "8 am");
        Span span7 = new Span("sentence_two", 0, 12, NlpPredicate.NlpTokenType.Date.toString(), "Aug 16 , 2016");
        Span span8 = new Span("sentence_one", 14, 28, NlpPredicate.NlpTokenType.Person.toString(), "Kelly Clarkson");
        Span span9 = new Span("sentence_two", 0, 14, NlpPredicate.NlpTokenType.Person.toString(), "Shirley Temple");
        
        IField[] fields1 = { new TextField("Microsoft, Google and Facebook are organizations."),
                new TextField("Donald Trump and Barack Obama are persons") };
        IField[] fields2 = { new TextField("I made an appointment at 8 am."), 
                new TextField("Aug 16, 2016 is a really important date.")};
        IField[] fields3 = { new TextField("I really love Kelly Clarkson's Because of You."),
                new TextField("Shirley Temple is a very famous actress.")};
        
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
        ITuple tuple2 = new DataTuple(SCHEMA_TWO_SENTENCE, fields2);
        ITuple tuple3 = new DataTuple(SCHEMA_TWO_SENTENCE, fields3);
        
        Schema returnSchema1 = Utils.createSpanSchema(tuple1.getSchema());
        Schema returnSchema2 = Utils.createSpanSchema(tuple2.getSchema());
        Schema returnSchema3 = Utils.createSpanSchema(tuple3.getSchema());
        
        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);
        resultList.add(Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema1));
        
        spanList.clear();
        spanList.add(span6);
        spanList.add(span7);
        resultList.add(Utils.getSpanTuple(tuple2.getFields(), spanList, returnSchema2));
        
        spanList.clear();
        spanList.add(span8);
        spanList.add(span9);
        resultList.add(Utils.getSpanTuple(tuple3.getFields(), spanList, returnSchema3));
        
        return resultList;
    }
}
