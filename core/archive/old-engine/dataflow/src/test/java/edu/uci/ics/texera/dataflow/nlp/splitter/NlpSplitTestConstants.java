package edu.uci.ics.texera.dataflow.nlp.splitter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class NlpSplitTestConstants {
    
    public static String TEXT = "text";
    
    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);
    public static Schema SPLIT_SCHEMA = new Schema(TEXT_ATTRIBUTE);
    
    private static String sentence1 =  "1.3 billion equals 1300 million .";
    private static String sentence2 =  "Mr. Wayne does not split .";
    

    
    public static List<Tuple> getOneToOneTestTuple() throws ParseException {
        IField[] fields1 = { new TextField(sentence1 + sentence2) };
        Tuple tuple1 = new Tuple(SPLIT_SCHEMA, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getOneToManyTestTuple() throws ParseException {
        IField[] fields1 = { new TextField(sentence1 + sentence2) };
        Tuple tuple1 = new Tuple(SPLIT_SCHEMA, fields1);
        return Arrays.asList(tuple1);
    }
    
    public static List<Tuple> getOneToOneResultTuple() throws ParseException {
        // Build the expected result Tuple
        List<Span> spanList = new ArrayList<Span>();
        
        Span span1 = new Span(TEXT, 0, sentence1.length(), PropertyNameConstants.NLP_SPLIT_KEY, sentence1);
        spanList.add(span1);
        Span span2 = new Span(TEXT, sentence1.length()+1, sentence1.length()+sentence2.length()+1, PropertyNameConstants.NLP_SPLIT_KEY, sentence2);
        spanList.add(span2);

        Tuple tuple1 = getOneToOneTestTuple().get(0);
        Tuple returnTuple = new Tuple.Builder(tuple1).add(SchemaConstants.SPAN_LIST_ATTRIBUTE, new ListField<Span>(spanList)).build();

        return Arrays.asList(returnTuple);
    }
    
    public static List<Tuple> getOneToManyResultTuple() throws ParseException {
        // Build the expected result Tuple
        List<Tuple> resultList = new ArrayList<>();
        
        IField sentenceText1 = new TextField(sentence1);
        Tuple returnTuple1 = new Tuple.Builder(getOneToManyTestTuple().get(0))
                .add(PropertyNameConstants.NLP_OUTPUT_TYPE, AttributeType.TEXT, sentenceText1).build();
        resultList.add(returnTuple1);
        
        
        IField sentenceText2 = new TextField(sentence2);
        Tuple returnTuple2 = new Tuple.Builder(getOneToManyTestTuple().get(0))
                .add(PropertyNameConstants.NLP_OUTPUT_TYPE, AttributeType.TEXT, sentenceText2).build();
        resultList.add(returnTuple2);      

        return resultList;
    }
}
