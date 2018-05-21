package edu.uci.ics.texera.dataflow.twitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;

public class TwitterConverter implements IOperator {
    
    private final String rawDataAttribute = AsterixSource.RAW_DATA;
    
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public TwitterConverter() {
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
        Tuple tuple;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            List<IField> tweetFields = generateFieldsFromJson(
                    tuple.getField(rawDataAttribute).getValue().toString());
            if (! tweetFields.isEmpty()) {
                cursor++;
                
                List<IField> tupleFields = new ArrayList<>();
                
                final Tuple finalTuple = tuple;
                tupleFields.addAll(tuple.getSchema().getAttributeNames().stream()
                        .filter(attrName -> ! attrName.equalsIgnoreCase(rawDataAttribute))
                        .map(attrName -> finalTuple.getField(attrName, IField.class))
                        .collect(Collectors.toList()));
                tupleFields.addAll(tweetFields);
                return new Tuple(outputSchema, tupleFields);
            }
        }
        return null;
    }
    
    private List<IField> generateFieldsFromJson(String rawJsonData) {
        try {
            JsonNode tweet = new ObjectMapper().readTree(rawJsonData);
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
            return Arrays.asList(
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
                    new StringField(createAt));
        } catch (Exception e) {
            return Arrays.asList();
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

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        if (! inputSchema[0].containsAttribute(rawDataAttribute)) {
            throw new DataflowException(String.format(
                    "raw twitter attribute %s is not present in the input schema %s",
                    rawDataAttribute, inputSchema[0].getAttributeNames()));
        }
        ArrayList<Attribute> outputAttributes = new ArrayList<>();
        outputAttributes.addAll(inputSchema[0].getAttributes().stream()
                .filter(attr -> ! attr.getName().equalsIgnoreCase(rawDataAttribute))
                .collect(Collectors.toList()));
        outputAttributes.addAll(TwitterConverterConstants.additionalAttributes);
        return new Schema(outputAttributes);
    }
}
