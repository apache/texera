package edu.uci.ics.texera.api.tuple;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uci.ics.texera.api.constants.JsonConstants;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

/**
 * This class is a custom Json Deserializer for the Tuple class.
 * 
 * The Tuple class cannot use the default Deserializer because there are many
 *   kinds of fields, and the field doesn't have information of the FieldType.
 * This custom deserializer can use the schema information to process the JSON.
 *   
 * For example, a tuple has an IDField and a TextField, the JSON representation is:
 * (IDField):   {"value": "id of the tuple"}  ("_id" is also a string)
 * (TextField): {"value": "some text in the field"}
 * There's no difference between the representation of IDField and TextField.
 * 
 * However, a tuple also contains its schema: 
 * {"attributes": [
 *     {"attributeName": "_id", "attributeType": "_id"},
 *     {"attributeName": "textAttr", "attributeType": " text"}
 * ]}
 * Therefore, this custom deserializer can use the information:
 *   the first field is an IDField, the second field is TextField,
 *   to map each field to the right class.
 * 
 * @author Zuozhi Wang
 *
 */
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
        for (int i = 0; i < schema.getAttributes().size(); i++) {
            AttributeType attributeType = schema.getAttributes().get(i).getType();
            JsonNode fieldNode = fieldsNode.get(i);
            IField field = new ObjectMapper().treeToValue(fieldNode, attributeType.getFieldClass());
            fields.add(field);
        }
        return new Tuple(schema, fields);
    }


}
