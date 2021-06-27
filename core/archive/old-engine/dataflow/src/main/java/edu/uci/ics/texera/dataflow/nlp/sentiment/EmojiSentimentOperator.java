package edu.uci.ics.texera.dataflow.nlp.sentiment;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This Operator performs sentiment analysis using Simple Emoji and Emoticon analysis targeted specifically for tweets
 * using unicode, browser escape character and smiley regex matching.
 *
 * The result is an integer indicating the sentiment score, which represents:
 *
 * 1 - positive
 * 0 - neutral
 * -1 - negative
 *
 *
 * The result will be put into an attribute with resultAttributeName specified in predicate, and type Integer.
 *
 * @author Vinay Bagade
 *
 */
public class EmojiSentimentOperator implements IOperator {
    private final EmojiSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    //SMILEY_REGEX_PATTERN identifies all happiness related emoticons like :) :-) <3 etc in the given text. 
    //The regex is given below. 
    public static final Pattern SMILEY_REGEX_PATTERN = Pattern.compile(".*(:[)DdpP]|:[ -]\\)|<3)+.*");
    //FROWNY_REGEX_PATTERN identifies all sadness related emoticons like :( :-( etc.
    //The regex is given below.
    public static final Pattern FROWNY_REGEX_PATTERN = Pattern.compile(".*(:[(<]|:[ -]\\()+.*");
    //Sometimes in chats and tweets the regex typed by users get converted to javascript escape characters. 
    //The range of these escape characters is given below in the EMOJI_REGEX pattern. 
    public static final Pattern EMOJI_REGEX = Pattern.compile(".*([\uD83C-\uDBFF\uDC00-\uDFFF])+.*");
    //Below is the list of all happy emoticon unicode characters.
    static ArrayList<String> happy = new ArrayList<String>(Arrays.asList("1f601", "1f602", "1f603", "1f604", "1f605",
            "1f606", "1f609", "1f60A", "1f60B", "1f60D", "1f618", "1f61A", "1f61C", "1f61D", "1f624", "1f632", "1f638",
            "1f639", "1f63A", "1f63B", "1f63D", "1f647", "1f64B", "1f64C", "1f64F", "U+270C", "U+2728", "U+2764", "U+263A",
            "U+2665", "U+3297", "1f31F", "1f44F", "1f48B", "1f48F", "1f491", "1f492", "1f493", "1f495", "1f496", "1f497",
            "1f498", "1f499", "1f49A", "1f49B", "1f49C", "1f49D", "1f49D", "1f49F", "1f4AA", "1f600", "1f607",
            "1f608", "1f60E", "1f617", "1f619", "1f61B", "1f31E", "1f60C", "1f60F", "1f633", "1f63C", "1f646",
            "U+2B50", "1f44D", "1f44C"));
    //Below is the list of all neutral emoticon unicode characters.
    static ArrayList<String> neutral = new ArrayList<String>(Arrays.asList("1f614", "1f623", "U+2753", "U+2754", "1f610", "1f611",
            "1f62E", "1f636"));
    //Below is the list of all unhappy emoticon unicode characters. 
    //The list can be found at http://unicode.org/emoji/charts/emoji-ordering.html
    static ArrayList<String> unhappy = new ArrayList<String>(Arrays.asList("1f612", "1f613", "1f616", "1f61E", "1f625", "1f628",
            "1f62A", "1f62B", "1f637", "1f635", "1f63E", "U+26A0", "1f44E", "1f4A4",
            "1f615", "1f61F", "1f62F", "1f634","1f620", "1f621", "1f622", "1f629",
            "1f62D", "1f630", "1f631", "1f63F", "1f640", "1f645", "1f64D", "1f64E",
            "U+274C", "U+274E", "1f494", "1f626", "1f627", "1f62C","U+2639","1f641",
            "1f616","1f61E","1f61F","1f624","1f622","1f62D","1f626","1f627",
            "1f628","1f629","1f92F","1f62C","1f630","1f631","1f633","1f92A",
            "1f635","1f621","1f620","1f92C"));
    
    public EmojiSentimentOperator(EmojiSentimentPredicate predicate) {
        this.predicate = predicate;
    }

    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {
            throw new TexeraException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    private Schema transformSchema(Schema inputSchema) {
        Schema.checkAttributeExists(inputSchema, predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());
        return new Schema.Builder().add(inputSchema).add(predicate.getResultAttributeName(), AttributeType.INTEGER).build();
    }
    
    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();

        // generate output schema by transforming the input schema
        outputSchema = transformToOutputSchema(inputSchema);

        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }

        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(inputTuple.getFields());
        outputFields.add(new IntegerField(computeSentimentScore(inputTuple)));

        return new Tuple(outputSchema, outputFields);

    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
    
    /*The following function computes the sentiment score of the given field of a tuple.The function first checks if 
    there is a smiley related regex pattern in the text followed by a frowny regex pattern it adds a point if smiley 
    pattern is found and subtracts point for frowny regex pattern. If none of them are found it checks for javascript
    escape characters in range defined by EMOJI_REGEX . If escape characters are found it converts them into unicode
    string to check which if the unicode string is contained in the happy list, sad list or neutral Arraylist of unicode
    strings and increments or decrements score appropriately.*/ 
    
    
    private Integer computeSentimentScore(Tuple inputTuple) {
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        Matcher matcher = null;
        Integer matchedStringScore = SentimentConstants.NEUTRAL;
        if(SMILEY_REGEX_PATTERN!= null){
            matcher = SMILEY_REGEX_PATTERN.matcher(inputText);
            if(matcher.matches()){
                matchedStringScore++;
            }
        }
        if(FROWNY_REGEX_PATTERN!= null){
            matcher = FROWNY_REGEX_PATTERN.matcher(inputText);
            if(matcher.matches()){
                matchedStringScore--;
            }
        }
        if (EMOJI_REGEX != null) {
            matcher = EMOJI_REGEX.matcher(inputText);
            if(matcher.matches()) {
                for( int i = 0; i < matcher.groupCount(); i++ ) {
                    String matchedString = matcher.group(i);
                    char[] ca = matchedString.toCharArray();
                    //if javascript escape characters in range of EMOJI_REGEX are found it loops through the entire strings to check 
                    // for presence of emoticon unicode in corrosponding arraylists. A unicodestring is made of two adjacent chars.
                    for(int j = 0; j < ca.length-1; j++  ) {
                        String unicodeString = String.format("%04x", Character.toCodePoint(ca[j], ca[j+1]));
                        //check if the uncode string is present in the any one of the arraylists
                        if(happy.contains( unicodeString )) {
                            matchedStringScore++;
                        } else if(neutral.contains( unicodeString )){
                            // neutral doesn't affect the score
                        } else if(unhappy.contains( unicodeString )){
                            matchedStringScore--;
                        }
                    }
                }
            }
        }
        if(matchedStringScore < SentimentConstants.NEUTRAL){
            matchedStringScore = SentimentConstants.NEGATIVE;
        }
        if(matchedStringScore > SentimentConstants.NEUTRAL){
            matchedStringScore = SentimentConstants.POSITIVE;
        }
        return matchedStringScore;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {

        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));


        // check if input schema is present
        if (! inputSchema[0].containsAttribute(predicate.getInputAttributeName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema[0].getAttributeNames()));
        }

        // check if attribute type is valid
        AttributeType inputAttributeType =
                inputSchema[0].getAttribute(predicate.getInputAttributeName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) ||
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new TexeraException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }

        return transformSchema(inputSchema[0]);
    }
}
