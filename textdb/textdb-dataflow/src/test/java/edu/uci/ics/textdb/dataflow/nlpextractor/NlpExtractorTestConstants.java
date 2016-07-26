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
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;

/**
 * Created by Sam on 16/4/27.
 */
public class NlpExtractorTestConstants {


    public static final String SENTENCE_ONE = "sentence_one";
    public static final String SENTENCE_TWO = "sentence_two";


    public static final Attribute SENTENCE_ONE_ATTR = new Attribute(SENTENCE_ONE, FieldType.TEXT);

    public static final Attribute SENTENCE_TWO_ATTR = new Attribute(SENTENCE_TWO, FieldType.TEXT);


    public static final List<Attribute> ATTRIBUTES_ONE_SENTENCE = Arrays.asList(
            SENTENCE_ONE_ATTR);

    public static final List<Attribute> ATTRIBUTES_TWO_SENTENCE = Arrays.asList(
            SENTENCE_ONE_ATTR, SENTENCE_ONE_ATTR);

    public static final Schema SCHEMA_ONE_SENTENCE = new Schema(SENTENCE_ONE_ATTR);
    public static final Schema SCHEMA_TWO_SENTENCE = new Schema(SENTENCE_ONE_ATTR, SENTENCE_TWO_ATTR);


    public static List<ITuple> getTest1Tuple() throws ParseException {
        IField[] fields1 = {new TextField("Microsoft is an organization.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }


    public static List<ITuple> getTest2Tuple() throws ParseException {


        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest3Tuple() throws ParseException {

        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }

    public static List<ITuple> getTest4Tuple() throws ParseException {

        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations."), new TextField("Donald Trump and Barack Obama are persons")};
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }


    public static List<ITuple> getTest7Tuple() throws ParseException {
        IField[] fields1 = {new TextField("Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);
        return Arrays.asList(tuple1);
    }


    public static List<ITuple> getTest1ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpExtractor.NlpTokenType.Organization.toString(), "Microsoft");
        spanList.add(span1);

        IField[] fields1 = {new TextField("Microsoft is an organization.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest2ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpExtractor.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpExtractor.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpExtractor.NlpTokenType.Organization.toString(), "Facebook");
        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);


        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);
        return resultList;
    }

    public static List<ITuple> getTest3ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpExtractor.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpExtractor.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpExtractor.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_one", 53, 65, NlpExtractor.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_one", 70, 82, NlpExtractor.NlpTokenType.Person.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }


    public static List<ITuple> getTest4ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();
        Span span1 = new Span("sentence_one", 0, 9, NlpExtractor.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpExtractor.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpExtractor.NlpTokenType.Organization.toString(), "Facebook");
        Span span4 = new Span("sentence_two", 0, 12, NlpExtractor.NlpTokenType.Person.toString(), "Donald Trump");
        Span span5 = new Span("sentence_two", 17, 29, NlpExtractor.NlpTokenType.Person.toString(), "Barack Obama");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);
        spanList.add(span4);
        spanList.add(span5);

        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations."), new TextField("Donald Trump and Barack Obama are persons")};
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }


    public static List<ITuple> getTest5ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_two", 0, 12, NlpExtractor.NlpTokenType.Person.toString(), "Donald Trump");
        Span span2 = new Span("sentence_two", 17, 29, NlpExtractor.NlpTokenType.Person.toString(), "Barack Obama");


        spanList.add(span1);
        spanList.add(span2);
        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations."), new TextField("Donald Trump and Barack Obama are persons")};
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

    public static List<ITuple> getTest6ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 0, 9, NlpExtractor.NlpTokenType.Organization.toString(), "Microsoft");
        Span span2 = new Span("sentence_one", 11, 17, NlpExtractor.NlpTokenType.Organization.toString(), "Google");
        Span span3 = new Span("sentence_one", 22, 30, NlpExtractor.NlpTokenType.Organization.toString(), "Facebook");

        spanList.add(span1);
        spanList.add(span2);
        spanList.add(span3);


        IField[] fields1 = {new TextField("Microsoft, Google and Facebook are organizations."), new TextField("Donald Trump and Barack Obama are persons")};
        ITuple tuple1 = new DataTuple(SCHEMA_TWO_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }


    public static List<ITuple> getTest7ResultTuples() {
        List<ITuple> resultList = new ArrayList<>();
        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 12, 16, NlpExtractor.NlpTokenType.Adjective.toString(), "warm");
        spanList.add(span1);

        IField[] fields1 = {new TextField("Feeling the warm sun rays beaming steadily down, the girl decided there was no need to wear a coat.")};
        ITuple tuple1 = new DataTuple(SCHEMA_ONE_SENTENCE, fields1);

        Schema returnSchema = Utils.createSpanSchema(tuple1.getSchema());

        ITuple returnTuple = Utils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
}
