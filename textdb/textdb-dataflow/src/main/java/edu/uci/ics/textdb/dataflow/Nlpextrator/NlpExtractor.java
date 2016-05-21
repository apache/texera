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
 * @author Feng [sam0227] on 4/27/16.
 *         <p>
 *         Wrap the Stanford NLP Named Entity Recognizer as an operator.
 *         This operator would recognize 7 classes: Location, Person, Organization, Money, Percent, Date and Time.
 *         Return the recoginized data as a list of spans that are appended to the original tuple as a field.
 *         <p>
 *         For example: Given tuple with two fields: sentence1, sentence2.
 *         tuple: ["Google is an organization.", "Its headquarter is in Mountain View."]
 *         <p>
 *         Append a list of spans then return:
 *         ["sentence1,0,6,Google, NE_ORGANIZATION", "sentence2,22,25,Mountain View, NE_LOCATION"]
 */

public class NlpExtractor implements IOperator {


    private IOperator sourceOperator;
    private List<Attribute> searchInAttributes;
    private ITuple sourceTuple;
    private Schema returnSchema;
    private NlpConstants NlpConstant = null;
    private String flag = null;


    public enum NlpConstants {
        NE, Number, Location, Person, Organization, Money, Percent, Date, Time, Noun, Verb, Adjective, Adverb;

        private static boolean isPOSConstant(NlpConstants constant) {
            if (constant.equals(NlpConstants.Adjective) || constant.equals(NlpConstants.Adverb) || constant.equals(NlpConstants.Noun) || constant.equals(NlpConstants.Verb)) {
                return true;
            } else {
                return false;
            }
        }
    };


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


    /**
     * @about Opens Named Entity Extractor
     */
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
     * @about Return all named entities that are recognized in a document.
     * Return format is a Tuple that contains only one field which is
     * a list of spans of the result.
     * @overview First get a tuple from the source operator then process it
     * using the Stanford NLP package. for all recognized words, compute their
     * spans and return all as a list.
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
     * @return a List of spans of the extracted information
     * @about This function takes an (TextField) IField and a String
     * (the field's name) as input and uses the Stanford NLP package to process the field.
     * It returns a list of spans
     * In the returning span: Value -> the word itself
     * Key   -> NE_Constant
     * @overview Using the Stanford NLP package to process the textField value.
     * First set up a pipeline of Annotators for the string: TokenizerAnnotator,
     * SentencesAnnotator, PartOfSpeechAnnotator, LemmaAnnotator and
     * NamedEntityTagAnnotator. The order is mandatory because they have
     * dependencies. After the pipeline, each token is wrapped as a CoreLabel
     * and each sentence is wrapped as CoreMap. Each annotator adds its
     * annotation to the CoreMap(sentence) or CoreLabel(token) object.
     * <p>
     * After the pipeline is done, scan each CoreLabel(token) for
     * its NamedEntityAnnotation, if it's a valid value (not 'O'),
     * then makes it a span and add to the return list.
     * The Stanford NLP constants are mapped into the NE constants.
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
     * This function takes a Stanford NLP Constant (The 7 Classes as LOCATION,PERSON,ORGANIZATION,MONEY,PERCENT,DATE,
     * TIME and NUMBER) and returns the corresponding NE Constant.
     *
     * @param NLPConstant
     * @return
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


    /**
     * @about Closes the operator
     */
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
