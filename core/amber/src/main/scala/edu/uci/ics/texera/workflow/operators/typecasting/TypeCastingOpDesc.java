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
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;

import static java.util.Collections.singletonList;
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
    public AttributeType resultType;


    @Override
    public OneToOneOpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        if (attribute == null) {
            throw new RuntimeException("TypeCasting: attribute is null");
        }
        return new OneToOneOpExecConfig(operatorIdentifier(), worker -> new TypeCastingOpExec(this));
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
        if (this.resultType != null) {
            return AttributeTypeUtils.SchemaCasting(schemas[0], this.attribute, this.resultType);
        }

        return schemas[0];
    }
}
