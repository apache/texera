package edu.uci.ics.texera.dataflow.nlp.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;

/**
 * @author Feng Hong
 * @about Wrap the Stanford NLP as an operator to extract desired information
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
public class NlpEntityOperator extends AbstractSingleInputOperator {

    private NlpEntityPredicate predicate;

    private Schema inputSchema;
    
    private static StanfordCoreNLP posPipeline = null;
    private static StanfordCoreNLP nerPipeline = null;

    /**
     * @param predicate
     * @about The constructor of the NlpEntityOperator.The operator will only search
     *        within the attributes specified in predicate and return the same tokens that are
     *        recognized as the same input inputNlpEntityType. If the input token
     *        type is NlpEntityType.NE_ALL, return all tokens that are recognized
     *        as NamedEntity Token Types.
     */
    public NlpEntityOperator(NlpEntityPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeNames());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttribute());

        outputSchema = transformToOutputSchema(inputSchema);
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }
        
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : predicate.getAttributeNames()) {
            IField field = inputTuple.getField(attributeName);
            matchingResults.addAll(extractNlpSpans(field, attributeName));
        }

        if (matchingResults.isEmpty()) {
            return null;
        }
        return new Tuple.Builder(inputTuple)
                .add(predicate.getResultAttribute(), AttributeType.LIST, new ListField<Span>(matchingResults))
                .build();
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
     *           inputnlpEntityType that used in this package, then check if it
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
        if (getNlpTypeIndicator(predicate.getNlpEntityType()).equals("POS")) {
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
                if (getNlpTypeIndicator(predicate.getNlpEntityType()).equals("POS")) {
                    stanfordNlpConstant = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                } else {
                    stanfordNlpConstant = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                }

                NlpEntityType nlpEntityType = mapNlpEntityType(stanfordNlpConstant);
                if (nlpEntityType == null) {
                    continue;
                }
                if (predicate.getNlpEntityType().equals(NlpEntityType.NE_ALL) || predicate.getNlpEntityType().equals(nlpEntityType)) {
                    int start = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
                    int end = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);

                    Span span = new Span(attributeName, start, end, nlpEntityType.toString(), word);
                    if (spanList.size() >= 1 && (getNlpTypeIndicator(predicate.getNlpEntityType()).equals("NE_ALL"))) {
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
    
    private static String getNlpTypeIndicator(NlpEntityType nlpEntityType) {
        if (isPOSTokenType(nlpEntityType)) {
            return "POS";
        } else {
            return "NE_ALL";
        }
    }
    
    private static boolean isPOSTokenType(NlpEntityType nlpEntityType) {
        if (nlpEntityType.equals(NlpEntityType.ADJECTIVE) || nlpEntityType.equals(NlpEntityType.ADVERB)
                || nlpEntityType.equals(NlpEntityType.NOUN) || nlpEntityType.equals(NlpEntityType.VERB)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param stanfordConstant
     * @return
     * @about This function takes a Stanford NLP Constant (Named Entity 7
     *        classes: LOCATION,PERSON,ORGANIZATION,MONEY,PERCENT,DATE, TIME and
     *        NUMBER and Part of Speech Token Types) and returns the
     *        corresponding enum NlpEntityType. (For Part of Speech, we match all
     *        Stanford Constant to only 4 types: Noun, Verb, Adjective and
     *        Adverb.
     */
    private static NlpEntityType mapNlpEntityType(String stanfordConstant) {
        switch (stanfordConstant) {
        case "NUMBER":
            return NlpEntityType.NUMBER;
        case "LOCATION":
            return NlpEntityType.LOCATION;
        case "PERSON":
            return NlpEntityType.PERSON;
        case "ORGANIZATION":
            return NlpEntityType.ORGANIZATION;
        case "MONEY":
            return NlpEntityType.MONEY;
        case "PERCENT":
            return NlpEntityType.PERCENT;
        case "DATE":
            return NlpEntityType.DATE;
        case "TIME":
            return NlpEntityType.TIME;
        case "JJ":
            return NlpEntityType.ADJECTIVE;
        case "JJR":
            return NlpEntityType.ADJECTIVE;
        case "JJS":
            return NlpEntityType.ADJECTIVE;
        case "RB":
            return NlpEntityType.ADVERB;
        case "RBR":
            return NlpEntityType.ADVERB;
        case "RBS":
            return NlpEntityType.ADVERB;
        case "NN":
            return NlpEntityType.NOUN;
        case "NNS":
            return NlpEntityType.NOUN;
        case "NNP":
            return NlpEntityType.NOUN;
        case "NNPS":
            return NlpEntityType.NOUN;
        case "VB":
            return NlpEntityType.VERB;
        case "VBD":
            return NlpEntityType.VERB;
        case "VBG":
            return NlpEntityType.VERB;
        case "VBN":
            return NlpEntityType.VERB;
        case "VBP":
            return NlpEntityType.VERB;
        case "VBZ":
            return NlpEntityType.VERB;
        default:
            return null;
        }
    }

    @Override
    protected void cleanUp() throws TexeraException {
    }

    public NlpEntityPredicate getPredicate() {
        return this.predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.checkAttributeExists(inputSchema[0], predicate.getAttributeNames());
        Schema.checkAttributeNotExists(inputSchema[0], predicate.getResultAttribute());

        return new Schema.Builder().add(inputSchema[0]).add(predicate.getResultAttribute(), AttributeType.LIST).build();
    }
}
