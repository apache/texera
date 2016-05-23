package edu.uci.ics.textdb.dataflow.nlpextrator;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;

import java.util.*;


/**
 * @author Feng
 * @about Wrap the Stanford NLP as an operator to extractor desire information (Named Entities, Part of Speech).
 * This operator could recognize 7 Named Entity classes: Location, Person, Organization, Money, Percent, Date and Time.
 * It'll also detect 4 types of Part of Speech: Noun, Verb, Adjective and Adverb.
 * Return the extracted token as a list of spans and appends to the original tuple as a new field.
 * For example: Given tuple with two fields: sentence1, sentence2,  specify to extract all Named Entities.
 * Source Tuple: ["Google is an organization.", "Its headquarter is in Mountain View."]
 * Appends a list of spans as a field for the return tuple.:
 * ["sentence1,0,6,Google, Organization", "sentence2,22,25,Mountain View, Location"]
 */

public class NlpExtractor implements IOperator {


    private IOperator sourceOperator;
    private List<Attribute> searchInAttributes;
    private ITuple sourceTuple;
    private Schema returnSchema;
    private NlpConstants NlpConstant = null;
    private String flag = null;


    /**
     * Named Entity Constants: NE, Number, Location, Person, Organization, Money, Percent, Date, Time.
     * Part Of Speech Constants: Noun, Verb, Adjective, Adverb
     */
    public enum NlpConstants {
        NE, Number, Location, Person, Organization, Money, Percent, Date, Time, Noun, Verb, Adjective, Adverb;

        private static boolean isPOSConstant(NlpConstants constant) {
            if (constant.equals(NlpConstants.Adjective) || constant.equals(NlpConstants.Adverb) || constant.equals(NlpConstants.Noun) || constant.equals(NlpConstants.Verb)) {
                return true;
            } else {
                return false;
            }
        }
    }

    ;


    /**
     * @param operator
     * @param searchInAttributes
     * @param nlpConstant
     * @throws DataFlowException
     * @about The constructor of the NlpExtractor. Allow users to pass a list of attributes and a NlpConstant.
     * The operator will only search within the attributes and return the same token that recognized as the same input
     * NlpConstant. IF the input constant is NlpConstants.NE, return all tokens that recognized as NamedEntity Constants.
     */
    public NlpExtractor(IOperator operator, List<Attribute> searchInAttributes, NlpConstants nlpConstant) throws DataFlowException {
        this.sourceOperator = operator;
        this.searchInAttributes = searchInAttributes;
        this.NlpConstant = nlpConstant;
        if (NlpConstants.isPOSConstant(nlpConstant)) {
            flag = "POS";
        } else {
            flag = "NE";
        }
    }


    @Override
    public void open() throws Exception {
        try {
            sourceOperator.open();
            returnSchema = null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }


    /**
     * @about Return the extracted data as a list of spans
     * and appends to the original tuple as a new field.
     * @overview Get a tuple from the source operator
     * Use the Stanford NLP package to process specified fields.
     * For all recognized tokens that match the input constant,
     * create their spans and make them as a list. Appends the list
     * as a field in the original tuple.
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        sourceTuple = sourceOperator.getNextTuple();
        if (sourceTuple == null) {
            return null;
        } else {
            if (returnSchema == null) {
                returnSchema = Utils.createSpanSchema(sourceTuple.getSchema());
            }
            List<Span> spanList = new ArrayList<>();
            for (Attribute attribute : searchInAttributes) {
                String fieldName = attribute.getFieldName();
                IField field = sourceTuple.getField(fieldName);
                spanList.addAll(extractInfoSpans(field, fieldName));
            }

            ITuple returnTuple = Utils.getSpanTuple(sourceTuple.getFields(), spanList, returnSchema);
            sourceTuple = sourceOperator.getNextTuple();
            return returnTuple;
        }
    }

    /**
     * @param iField
     * @param fieldName
     * @return
     * @about This function takes an IField(TextField) and a String
     * (the field's name) as input and uses the Stanford NLP package
     * to process the field based on the input constant and flag.
     * In the result spans, value represents the word itself
     * and key represents the recognized constant
     * @overview First set up a pipeline of Annotators based on the flag.
     * If the flag is "NE", we set up the NamedEntityTagAnnotator,
     * if it's "POS", then only PartOfSpeechAnnotator is needed.
     * <p>
     * The pipeline has to be this order: TokenizerAnnotator,
     * SentencesAnnotator, PartOfSpeechAnnotator, LemmaAnnotator and
     * NamedEntityTagAnnotator.
     * <p>
     * In the pipeline, each token is wrapped as a CoreLabel
     * and each sentence is wrapped as CoreMap. Each annotator adds its
     * annotation to the CoreMap(sentence) or CoreLabel(token) object.
     * <p>
     * After the pipeline, scan each CoreLabel(token) for its
     * NamedEntityAnnotation or PartOfSpeechAnnotator depends on the flag
     * <p>
     * For each Stanford NLP annotation, get it's corresponding NlpConstant
     * that used in this package, then check if it equals to the input constant.
     * If yes, makes it a span and add to the return list.
     * <p>
     * The NLP package has annotations for the start and end position of a token
     * and it perfectly matches the span design so we just use them.
     */
    private List<Span> extractInfoSpans(IField iField, String fieldName) {
        List<Span> spanList = new ArrayList<>();
        String text = (String) iField.getValue();
        Properties props = new Properties();

        if (flag.equals("POS")) {
            props.setProperty("annotators", "tokenize, ssplit, pos");
        } else {
            props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
        }

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation documentAnnotation = new Annotation(text);
        pipeline.annotate(documentAnnotation);
        List<CoreMap> sentences = documentAnnotation.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {

                String StanfordNlpConstant;
                if (flag.equals("POS")) {
                    StanfordNlpConstant = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                } else {
                    StanfordNlpConstant = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                }


                NlpConstants thisNlpConstant = getInfoConstant(StanfordNlpConstant);
                if (thisNlpConstant == null) {
                    continue;
                }
                if (NlpConstant.equals(NlpConstants.NE) || NlpConstant.equals(thisNlpConstant)) {
                    int start = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
                    int end = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);

                    Span span = new Span(fieldName, start, end, thisNlpConstant.toString(), word);

                    if (spanList.size() >= 1 && (flag.equals("NE"))) {
                        Span previousSpan = spanList.get(spanList.size() - 1);
                        if (previousSpan.getFieldName().equals(span.getFieldName())
                                && (span.getStart() - previousSpan.getEnd() <= 1)
                                && previousSpan.getKey().equals(span.getKey())) {
                            Span newSpan = mergeTwoSpans(previousSpan, span);
                            span = newSpan;
                            spanList.remove(spanList.size() - 1);
                        }
                    }
                    spanList.add(span);
                }

            }

        }

        return spanList;
    }


    /**
     * @param previousSpan
     * @param currentSpan
     * @return
     * @about This function takes two spans as input and merges them as a new span
     * <p>
     * Two spans with fieldName, start, end, key, value:
     * previousSpan: "Doc1", 10, 13, "Location", "New"
     * currentSpan : "Doc1", 14, 18, "Location", "York"
     * <p>
     * Would be merge to:
     * return:   "Doc1", 10, 18, "Location", "New York"
     * <p>
     * The caller needs to make sure:
     * 1. The two spans are adjacent.
     * 2. The two spans are in the same field. They should have the same fieldName.
     * 3. The two spans have the same key (Organization, Person,... etc)
     */
    private Span mergeTwoSpans(Span previousSpan, Span currentSpan) {
        String newWord = previousSpan.getValue() + " " + currentSpan.getValue();
        return new Span(previousSpan.getFieldName(), previousSpan.getStart(), currentSpan.getEnd(), previousSpan.getKey(), newWord);
    }


    /**
     * @param NLPConstant
     * @return
     * @about This function takes a Stanford NLP Constant (Named Entity 7 classes: LOCATION,PERSON,ORGANIZATION,MONEY,PERCENT,DATE,
     * TIME and NUMBER and Part of Speech Constants) and returns the corresponding enum type NlpConstant.
     * (For Part of Speech, we match all Stanford Constant to only 4 types: Noun, Verb, Adjective and Adverb.
     */
    private NlpConstants getInfoConstant(String NLPConstant) {
        switch (NLPConstant) {
            case "NUMBER":
                return NlpConstants.Number;
            case "LOCATION":
                return NlpConstants.Location;
            case "PERSON":
                return NlpConstants.Person;
            case "ORGANIZATION":
                return NlpConstants.Organization;
            case "MONEY":
                return NlpConstants.Money;
            case "PERCENT":
                return NlpConstants.Percent;
            case "DATE":
                return NlpConstants.Date;
            case "TIME":
                return NlpConstants.Time;
            case "JJ":
                return NlpConstants.Adjective;
            case "JJR":
                return NlpConstants.Adjective;
            case "JJS":
                return NlpConstants.Adjective;
            case "RB":
                return NlpConstants.Adverb;
            case "RBR":
                return NlpConstants.Adverb;
            case "RBS":
                return NlpConstants.Adverb;
            case "NN":
                return NlpConstants.Noun;
            case "NNS":
                return NlpConstants.Noun;
            case "NNP":
                return NlpConstants.Noun;
            case "NNPS":
                return NlpConstants.Noun;
            case "VB":
                return NlpConstants.Verb;
            case "VBD":
                return NlpConstants.Verb;
            case "VBG":
                return NlpConstants.Verb;
            case "VBN":
                return NlpConstants.Verb;
            case "VBP":
                return NlpConstants.Verb;
            case "VBZ":
                return NlpConstants.Verb;
            default:
                return null;
        }
    }


    @Override
    public void close() throws DataFlowException {
        try {
            NlpConstant = null;
            searchInAttributes = null;
            sourceTuple = null;
            returnSchema = null;
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
