package edu.uci.ics.texera.workflow.operators.typeCasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import java.util.List;
import java.util.stream.Collectors;

public class TypeCastingOpDesc extends MapOpDesc {
    @JsonProperty(value = "attribute", required = true)
    @JsonPropertyDescription("Type to perform casting")
    public String attribute;

    @JsonProperty(value = "CastingType", required = true)
    @JsonPropertyDescription("Result type you want to put")
    public CastingType resultType;


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
                "TypeCasting",
                "Casting type to another type",
                OperatorGroupConstants.UTILITY_GROUP(),
                1, 1
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        if (this.resultType!=null && this.attribute!=null){
            switch (this.resultType) {
                case STRING:
                    return Schema.newBuilder().add(schemas[0]).removeIfExists(this.attribute).add(this.attribute, AttributeType.STRING).build();
                case BOOLEAN:
                    return Schema.newBuilder().add(schemas[0]).removeIfExists(this.attribute).add(this.attribute, AttributeType.BOOLEAN).build();
                case DOUBLE:
                    return Schema.newBuilder().add(schemas[0]).removeIfExists(this.attribute).add(this.attribute, AttributeType.DOUBLE).build();
                case INTEGER:
                    return Schema.newBuilder().add(schemas[0]).removeIfExists(this.attribute).add(this.attribute, AttributeType.INTEGER).build();
            }
        }
        return Schema.newBuilder().add(schemas[0]).build();
    }
}
