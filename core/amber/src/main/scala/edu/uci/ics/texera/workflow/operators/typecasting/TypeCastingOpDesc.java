package edu.uci.ics.texera.workflow.operators.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class TypeCastingOpDesc extends MapOpDesc {
    @JsonProperty(required = true)
    @JsonSchemaTitle("attribute")
    @JsonPropertyDescription("Attribute for type casting")
    @AutofillAttributeName
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
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        List<Attribute> attributes = schemas[0].getAttributes();
        List<String> attributeNames = schemas[0].getAttributeNames();
        List<AttributeType> attributeTypes = attributes.stream().map(attr -> attr.getType()).collect(toList());
        Schema.Builder builder = Schema.newBuilder();
        // this loop check whether the current attribute in the array is the attribute for casting,
        // if it is, change it to result type
        // if it's not, remain the same type
        // we need this loop to keep the order the same as the original
        for (int i=0;i<attributes.size();i++) {
            if (attributeNames.get(i).equals(attribute)) {
                if (this.resultType != null){
                    switch (this.resultType) {
                        case STRING:
                            builder.add(this.attribute, AttributeType.STRING);
                            break;
                        case BOOLEAN:
                            builder.add(this.attribute, AttributeType.BOOLEAN);
                            break;
                        case DOUBLE:
                            builder.add(this.attribute, AttributeType.DOUBLE);
                            break;
                        case INTEGER:
                            builder.add(this.attribute, AttributeType.INTEGER);
                            break;
                        default:
                            throw new RuntimeException("Fail to change current AttributeType to result AttributeType in the schema");
                    }
                }

            } else {
                builder.add(attributeNames.get(i), attributeTypes.get(i));
            }
        }

        return builder.build();
    }
}
