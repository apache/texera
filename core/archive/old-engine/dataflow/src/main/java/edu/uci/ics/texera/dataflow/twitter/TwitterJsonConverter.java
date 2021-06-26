package edu.uci.ics.texera.dataflow.twitter;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DateTimeField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * Twitter Converter converts a JSON string representation of a tweet,
 *  flattens it into many Texera Fields, such as text, author, link, etc...
 *  and removes the original raw JSON data field.
 *  
 * See {@link TwitterJsonConverterConstants} for the full set of fields that this operator extracts,
 *  note that this operator does not keep all the information and fields in the original twitter JSON string. 
 * 
 * @author Zuozhi Wang
 *
 */
public class TwitterJsonConverter implements IOperator {
    
    private TwitterJsonConverterPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public TwitterJsonConverter(TwitterJsonConverterPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public void open() throws TexeraException {
        if (cursor == OPENED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        Tuple inputTuple;
        while ((inputTuple = inputOperator.getNextTuple()) != null) {

            String rawTwitterJsonString = inputTuple.getField(this.predicate.getTwitterJsonStringAttributeName()).getValue().toString();
            Optional<List<IField>> convertedTwitterFields = this.generateFieldsFromJson(rawTwitterJsonString);

            if (convertedTwitterFields.isPresent()) {
                cursor++;
                return new Tuple.Builder(inputTuple)
                        // remove the raw json field
                        .remove(this.predicate.getTwitterJsonStringAttributeName())
                        // add the new fields
                        .add(TwitterJsonConverterConstants.additionalAttributes, convertedTwitterFields.get())
                        .build();
            }

        }
        return null;
    }

    /**
     * Generates Fields from the raw JSON tweet.
     * Returns Optional.Empty() if something goes wrong while parsing this tweet.
     */
    private Optional<List<IField>> generateFieldsFromJson(String rawJsonData) {
        try {
            // read the JSON string into a JSON object
            JsonNode tweet = new ObjectMapper().readTree(rawJsonData);
            
            // extract fields from the JSON object
            String text = tweet.get("text").asText();
            Long id = tweet.get("id").asLong();
            String tweetLink = "https://twitter.com/statuses/" + id;
            JsonNode userNode = tweet.get("user");
            String userScreenName = userNode.get("screen_name").asText();
            String userLink = "https://twitter.com/" + userScreenName;
            String userName = userNode.get("name").asText();
            String userDescription = userNode.get("description").asText();
            Integer userFollowersCount = userNode.get("followers_count").asInt();
            Integer userFriendsCount = userNode.get("friends_count").asInt();
            JsonNode geoTagNode = tweet.get("geo_tag");
            String state = geoTagNode.get("stateName").asText();
            String county = geoTagNode.get("countyName").asText();
            String city = geoTagNode.get("cityName").asText();
            String createAt = tweet.get("create_at").asText();
            ZonedDateTime zonedCreateAt = ZonedDateTime.parse(createAt, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));
            String isRetweet = tweet.get("is_retweet").asText();
            
            return Optional.of(Arrays.asList(
                    new StringField(id.toString()),
                    new TextField(text),
                    new StringField(tweetLink),
                    new StringField(userLink),
                    new TextField(userScreenName),
                    new TextField(userName),
                    new TextField(userDescription),
                    new IntegerField(userFollowersCount),
                    new IntegerField(userFriendsCount),
                    new TextField(state),
                    new TextField(county),
                    new TextField(city),
                    new DateTimeField(zonedCreateAt.toLocalDateTime()),
                    new StringField(isRetweet)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        inputOperator.close();
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    /**
     * Returns the transformed output schema of TwitterJsonConverter,
     *  removes the raw json attribute and adds the new additional attributes.
     */
    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        if (! inputSchema[0].containsAttribute(this.predicate.getTwitterJsonStringAttributeName())) {
            throw new DataflowException(String.format(
                    "raw twitter json attribute %s is not present in the input schema %s",
                    this.predicate.getTwitterJsonStringAttributeName(), inputSchema[0].getAttributeNames()));
        }

        return new Schema.Builder(inputSchema[0])
            .remove(this.predicate.getTwitterJsonStringAttributeName())
            .add(TwitterJsonConverterConstants.additionalAttributes)
            .build();
        
    }
}
