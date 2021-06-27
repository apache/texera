package edu.uci.ics.texera.api.field;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.span.Span;

/**
 * When the ListField<Span> is serialized to a Json String, the generic type (Span) information is lost.
 * When the same Json string is deserialized back to ListField, the value cannot be correctly converted to a Span 
 *   because Jackson doesn't know it should be mapped to the Span Class.
 * 
 * Since we only have Span List, this PR adds a custom deserializer to map any value to Span class, 
 *   so we can always get ListField<Span>.
 */
public class ListFieldJsonDeserializer extends StdDeserializer<ListField<?>> {

    private static final long serialVersionUID = 6079052875639039779L;
    
    public ListFieldJsonDeserializer() {
        this(null);
    }

    protected ListFieldJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    // TODO: currently all List<T> are SpanList. In the future maybe 
    //  either change List<T> to SpanList, or support generic list
    @Override
    public ListField<Span> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        JsonNode fieldValueNode = node.get(JsonConstants.FIELD_VALUE);
        
        ArrayList<Span> spanList = new ArrayList<>();
        for (int i = 0; i < fieldValueNode.size(); i++) {
            JsonNode spanValueNode = fieldValueNode.get(i);
            spanList.add(new ObjectMapper().convertValue(spanValueNode, Span.class));
        }
        return new ListField<Span>(spanList);
    }

}
