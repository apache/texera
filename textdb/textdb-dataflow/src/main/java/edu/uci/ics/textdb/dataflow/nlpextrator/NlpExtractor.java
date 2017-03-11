package edu.uci.ics.textdb.dataflow.nlpextrator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate.NlpTokenType;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;

/**
 * @author Feng Hong
 * @about Wrap the Stanford NLP as an operator to extractor desired information
 *        (Named Entities, Part of Speech). This operator could recognize 7
 *        Named Entity classes: Location, Person, Organization, Money, Percent,
 *        Date and Time. It'll also detect 4 types of Part of Speech: Noun,
 *        Verb, Adjective and Adverb.Return the extracted tokens as a list of
 *        spans and appends to the original tuple as a new field. For example:
 *        Given tuple with two fields: sentence1, sentence2, specify to extract
 *        all Named Entities. Source Tuple: ["Google is an organization.", "Its
 *        headquarters are in Mountain View."] Appends a list of spans as a
 *        field for the returned tuple: ["sentence1,0,6,Google, Organization",
 *        "sentence2,24,37,Mountain View, Location"]
 */
public class NlpExtractor extends AbstractSingleInputOperator {

    private NlpPredicate predicate;

    private Schema inputSchema;
    
    private static StanfordCoreNLP posPipeline = null;
    private static StanfordCoreNLP nerPipeline = null;

    /**
     * @param NlpPredicate
     * @about The constructor of the NlpExtractor.The operator will only search
     *        within the attributes specified in predicate and return the same tokens that are
     *        recognized as the same input inputNlpTokenType. If the input token
     *        type is NlpTokenType.NE_ALL, return all tokens that are recognized
     *        as NamedEntity Token Types.
     */
    public NlpExtractor(NlpPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
        }
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            }            
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }
        
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : predicate.getAttributeNames()) {
            IField field = inputTuple.getField(attributeName);
            matchingResults.addAll(extractNlpSpans(field, attributeName));
        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);
        return inputTuple;
    }
    
    /**
     * @param iField
     * @param attributeName
     * @return
     * @about This function takes an IField(TextField) and a String (the field's
     *        name) as input and uses the Stanford NLP package to process the
     *        field based on the input token type and nlpTypeIndicator. In the
     *        result spans, value represents the word itself and key represents
     *        the recognized token type
     * @overview First set up a pipeline of Annotators based on the
     *           nlpTypeIndicator. If the nlpTypeIndicator is "NE_ALL", we set
     *           up the NamedEntityTagAnnotator, if it's "POS", then only
     *           PartOfSpeechAnnotator is needed.
     *           <p>
     *           The pipeline has to be this order: TokenizerAnnotator,
     *           SentencesAnnotator, PartOfSpeechAnnotator, LemmaAnnotator and
     *           NamedEntityTagAnnotator.
     *           <p>
     *           In the pipeline, each token is wrapped as a CoreLabel and each
     *           sentence is wrapped as CoreMap. Each annotator adds its
     *           annotation to the CoreMap(sentence) or CoreLabel(token) object.
     *           <p>
     *           After the pipeline, scan each CoreLabel(token) for its
     *           NamedEntityAnnotation or PartOfSpeechAnnotator depends on the
     *           nlpTypeIndicator
     *           <p>
     *           For each Stanford NLP annotation, get it's corresponding
     *           inputNlpTokenType that used in this package, then check if it
     *           equals to the input token type. If yes, makes it a span and add
     *           to the return list.
     *           <p>
     *           The NLP package has annotations for the start and end position
     *           of a token and it perfectly matches the span design so we just
     *           use them.
     *           <p>
     *           For Example: With TextField value: "Microsoft, Google and
     *           Facebook are organizations while Donald Trump and Barack Obama
     *           are persons", with attributeName: Sentence1 and inputTokenType is
     *           Organization. Since the inputTokenType require us to use
     *           NamedEntity Annotator in the Stanford NLP package, the
     *           nlpTypeIndicator would be set to "NE". The pipeline would set
     *           up to cover the Named Entity Recognizer. Then get the value of
     *           NamedEntityTagAnnotation for each CoreLabel(token).If the value
     *           is the token type "Organization", then it meets the
     *           requirement. In this case "Microsoft","Google" and "Facebook"
     *           will satisfy the requirement. "Donald Trump" and "Barack Obama"
     *           would have token type "Person" and do not meet the requirement.
     *           For each qualified token, create a span accordingly and add it
     *           to the returned list. In this case, token "Microsoft" would be
     *           span: ["Sentence1", 0, 9, Organization, "Microsoft"]
     */
    private List<Span> extractNlpSpans(IField iField, String attributeName) {
        List<Span> spanList = new ArrayList<>();
        String text = (String) iField.getValue();
        Properties props = new Properties();

        // Setup Stanford NLP pipeline based on nlpTypeIndicator
        StanfordCoreNLP pipeline = null;
        if (predicate.getNlpTypeIndicator().equals("POS")) {
            props.setProperty("annotators", "tokenize, ssplit, pos");
            if (posPipeline == null) {
                posPipeline = new StanfordCoreNLP(props);
            }
            pipeline = posPipeline;
        } else {
            props.setProperty("annotators", "tokenize, ssplit, pos, lemma, " + "ner");
            if (nerPipeline == null) {
                nerPipeline = new StanfordCoreNLP(props);
            }
            pipeline = nerPipeline;
        }
        Annotation documentAnnotation = new Annotation(text);
        pipeline.annotate(documentAnnotation);
        List<CoreMap> sentences = documentAnnotation.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {

                String stanfordNlpConstant;

                // Extract annotations based on nlpTypeIndicator
                if (predicate.getNlpTypeIndicator().equals("POS")) {
                    stanfordNlpConstant = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                } else {
                    stanfordNlpConstant = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                }

                NlpTokenType thisNlpTokenType = getNlpTokenType(stanfordNlpConstant);
                if (thisNlpTokenType == null) {
                    continue;
                }
                if (predicate.getNlpTokenType().equals(NlpTokenType.NE_ALL) || predicate.getNlpTokenType().equals(thisNlpTokenType)) {
                    int start = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
                    int end = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);

                    Span span = new Span(attributeName, start, end, thisNlpTokenType.toString(), word);
                    if (spanList.size() >= 1 && (predicate.getNlpTypeIndicator().equals("NE_ALL"))) {
                        Span previousSpan = spanList.get(spanList.size() - 1);
                        if (previousSpan.getAttributeName().equals(span.getAttributeName())
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
     * @about This function takes two spans as input and merges them as a new
     *        span
     *        <p>
     *        Two spans with attributeName, start, end, key, value: previousSpan:
     *        "Doc1", 10, 13, "Location", "New" currentSpan : "Doc1", 14, 18,
     *        "Location", "York"
     *        <p>
     *        Would be merge to: return: "Doc1", 10, 18, "Location", "New York"
     *        <p>
     *        The caller needs to make sure: 1. The two spans are adjacent. 2.
     *        The two spans are in the same field. They should have the same
     *        attributeName. 3. The two spans have the same key (Organization,
     *        Person,... etc)
     */
    private Span mergeTwoSpans(Span previousSpan, Span currentSpan) {
        String newWord = previousSpan.getValue() + " " + currentSpan.getValue();
        return new Span(previousSpan.getAttributeName(), previousSpan.getStart(), currentSpan.getEnd(),
                previousSpan.getKey(), newWord);
    }

    /**
     * @param stanfordConstant
     * @return
     * @about This function takes a Stanford NLP Constant (Named Entity 7
     *        classes: LOCATION,PERSON,ORGANIZATION,MONEY,PERCENT,DATE, TIME and
     *        NUMBER and Part of Speech Token Types) and returns the
     *        corresponding enum NlpTokenType. (For Part of Speech, we match all
     *        Stanford Constant to only 4 types: Noun, Verb, Adjective and
     *        Adverb.
     */
    private NlpTokenType getNlpTokenType(String stanfordConstant) {
        switch (stanfordConstant) {
        case "NUMBER":
            return NlpTokenType.Number;
        case "LOCATION":
            return NlpTokenType.Location;
        case "PERSON":
            return NlpTokenType.Person;
        case "ORGANIZATION":
            return NlpTokenType.Organization;
        case "MONEY":
            return NlpTokenType.Money;
        case "PERCENT":
            return NlpTokenType.Percent;
        case "DATE":
            return NlpTokenType.Date;
        case "TIME":
            return NlpTokenType.Time;
        case "JJ":
            return NlpTokenType.Adjective;
        case "JJR":
            return NlpTokenType.Adjective;
        case "JJS":
            return NlpTokenType.Adjective;
        case "RB":
            return NlpTokenType.Adverb;
        case "RBR":
            return NlpTokenType.Adverb;
        case "RBS":
            return NlpTokenType.Adverb;
        case "NN":
            return NlpTokenType.Noun;
        case "NNS":
            return NlpTokenType.Noun;
        case "NNP":
            return NlpTokenType.Noun;
        case "NNPS":
            return NlpTokenType.Noun;
        case "VB":
            return NlpTokenType.Verb;
        case "VBD":
            return NlpTokenType.Verb;
        case "VBG":
            return NlpTokenType.Verb;
        case "VBN":
            return NlpTokenType.Verb;
        case "VBP":
            return NlpTokenType.Verb;
        case "VBZ":
            return NlpTokenType.Verb;
        default:
            return null;
        }
    }

    @Override
    protected void cleanUp() throws TextDBException {
    }

    public NlpPredicate getPredicate() {
        return this.predicate;
    }

}
