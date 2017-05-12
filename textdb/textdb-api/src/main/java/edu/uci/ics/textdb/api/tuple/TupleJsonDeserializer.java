package edu.uci.ics.textdb.api.tuple;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uci.ics.textdb.api.constants.JsonConstants;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

public class TupleJsonDeserializer extends StdDeserializer<Tuple> {

    private static final long serialVersionUID = 6244351898113632246L;
    
    public TupleJsonDeserializer() {
        this(null);
    }

    public TupleJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Tuple deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        JsonNode schemaNode = node.get(JsonConstants.SCHEMA);
        JsonNode fieldsNode = node.get(JsonConstants.FIELDS);
        
        Schema schema = new ObjectMapper().treeToValue(schemaNode, Schema.class);
        ArrayList<IField> fields = new ArrayList<>();
        for (int i = 0; i < schema.size(); i++) {
            AttributeType attributeType = schema.getAttribute(i).getAttributeType();
            JsonNode fieldNode = fieldsNode.get(i);
            IField field = new ObjectMapper().treeToValue(fieldNode, attributeType.getFieldClass());
            fields.add(field);
        }
        return new Tuple(schema, fields);
    }


}
