package edu.uci.ics.texera.workflow.operators.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.util.List;
import java.util.stream.Collectors;

public class TypeCastingOpDesc extends MapOpDesc {
    @JsonProperty(required = true)
    @JsonSchemaTitle("attribute")
    @JsonPropertyDescription("Attribute for type casting")
    public String attribute;

    @JsonProperty(required = true)
    @JsonSchemaTitle("cast type")
    @JsonPropertyDescription("Result type after type casting")
    public TypeCastingAttributeType resultType;


    @Override
    public OneToOneOpExecConfig operatorExecutor() {
        if (attribute == null) {
            throw new RuntimeException("TypeCasting: attribute is null");
        }
        return new OneToOneOpExecConfig(operatorIdentifier(),worker -> new TypeCastingOpExec(this));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Type Casting",
                "Cast type to another type",
                OperatorGroupConstants.UTILITY_GROUP(),
                1, 1, false
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        List<Attribute> attributes = schemas[0].getAttributes();
        List<String> attributeNames = schemas[0].getAttributeNames();
        List<AttributeType> attributeTypes = attributes.stream().map(attr -> attr.getType()).collect(Collectors.toList());
        Schema.Builder builder = Schema.newBuilder();
        for (int i=0;i<attributes.size();i++) {
            if (attributeNames.get(i).equals(attribute)) {
                if (this.resultType!=null && this.attribute!=null){
                    switch (this.resultType) {
                        case STRING:
                            builder.add(this.attribute, AttributeType.STRING);
                        case BOOLEAN:
                            builder.add(this.attribute, AttributeType.BOOLEAN);
                        case DOUBLE:
                            builder.add(this.attribute, AttributeType.DOUBLE);
                        case INTEGER:
                            builder.add(this.attribute, AttributeType.INTEGER);
                    }
                }

            } else {
                builder.add(attributeNames.get(i), attributeTypes.get(i));
            }
        }

        return builder.build();
    }
}
