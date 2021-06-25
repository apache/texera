package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.regexmatcher.label.LabeledRegexProcessor;
import edu.uci.ics.texera.dataflow.regexmatcher.label.LabledRegexNoQualifierProcessor;

/**
 * Created by chenli on 3/25/16.
 * 
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {
    
    public enum RegexType {
        NO_LABELS, LABELED_WITHOUT_QUALIFIER, LABELED_WITH_QUALIFIERS
    }
    
    /*
     * Regex pattern for determining if the regex has labels.
     * Match "<" in the beginning, and ">" in the end.
     * Between the brackets "<>", there are one or more number of characters,
     *   but cannot be "<" or ">", or the "\" escape character.
     *   
     * For example: 
     *   "<drug1>": is a label
     *   "<drug\>1": is not a label because the closing bracket is escaped.
     *   "<a <drug> b>" : only the inner <drug> is treated as a label
     * 
     * TODO:
     * this regex can't handle escape inside a bracket pair:
     * <a\>b>: the semantic of this regex is, the label itself can be "a>b"
     */
    public static final String CHECK_REGEX_LABEL = "<[^<>\\\\]*>";
    
    /*
     * Regex pattern for determining if the regex has qualifiers.
     * 
     * TODO:
     * this regex doesn't handle qualifiers correct.
     * It only allows alphabets, digits, and backets.
     * But some characters like "_", "-", "=" doesn't have special meaning
     *   and shouldn't be treated as qualifiers.
     */
    public static final String CHECK_REGEX_QUALIFIER = "[^a-zA-Z0-9<> ]";
    
    
    private final RegexPredicate predicate;
    private RegexType regexType;
        
    private Pattern regexPattern;
    LabeledRegexProcessor labeledRegexProcessor;
    LabledRegexNoQualifierProcessor labledRegexNoQualifierProcessor;
    
    private boolean addResultAttribute = false;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataflowException {   
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        Schema inputSchema = inputOperator.getOutputSchema();

        this.addResultAttribute = predicate.getSpanListName() != null;
        
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeNames());
        if (addResultAttribute) { 
            Schema.checkAttributeNotExists(inputSchema, predicate.getSpanListName());
        }

        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());

        findRegexType();
        // Check if labeled or unlabeled
        if (this.regexType == RegexType.NO_LABELS) {
            regexPattern = predicate.isIgnoreCase() ? 
                    Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE)
                    : Pattern.compile(predicate.getRegex());
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            labeledRegexProcessor = new LabeledRegexProcessor(predicate);
        } else {
            labledRegexNoQualifierProcessor = new LabledRegexNoQualifierProcessor(predicate);
        }
    }
    
    /*
     * Determines the type of the regex: no_label / labeled_with_qualifier / labeled_without_qualifier
     */
    private void findRegexType() {
        Matcher labelMatcher = Pattern.compile(CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        if (! labelMatcher.find()) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        Matcher qualifierMatcher = Pattern.compile(CHECK_REGEX_QUALIFIER).matcher(predicate.getRegex());
        if (qualifierMatcher.find()) {
            regexType = RegexType.LABELED_WITH_QUALIFIERS;
        } else {
            regexType = RegexType.LABELED_WITHOUT_QUALIFIER;
        }
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

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     * 
     * @param inputTuple
     *            document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     *         in the document
     * @throws DataflowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws DataflowException {
        if (inputTuple == null) {
            return null;
        }

        List<Span> matchingResults = null;
        if (this.regexType == RegexType.NO_LABELS) {
            matchingResults = computeMatchingResultsWithPattern(inputTuple, predicate, regexPattern);
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            matchingResults = labeledRegexProcessor.computeMatchingResults(inputTuple);
        } else {
            matchingResults = labledRegexNoQualifierProcessor.computeMatchingResults(inputTuple);
        }
        
        if (matchingResults.isEmpty()) {
            return null;
        }
        
        Tuple.Builder tupleBuilder = new Tuple.Builder(inputTuple);

        if (addResultAttribute) {
            tupleBuilder.add(predicate.getSpanListName(), AttributeType.LIST, new ListField<Span>(matchingResults));
        }

        return tupleBuilder.build();
    }

    public static List<Span> computeMatchingResultsWithPattern(Tuple inputTuple, RegexPredicate predicate, Pattern pattern) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
            
            Matcher javaMatcher = pattern.matcher(fieldValue);
            while (javaMatcher.find()) {
                int start = javaMatcher.start();
                int end = javaMatcher.end();
                matchingResults.add(
                        new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            }
        }
        
        return matchingResults;
    }
    
    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.Builder outputSchemaBuilder = new Schema.Builder(inputSchema[0]);
        if (addResultAttribute) {
            outputSchemaBuilder.add(predicate.getSpanListName(), AttributeType.LIST);
        }
        return outputSchemaBuilder.build();
    }
}