package edu.uci.ics.texera.workflow.operators.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class TypeCastingOpDesc extends MapOpDesc {

    @JsonProperty(required = true)
    @JsonSchemaTitle("TypeCasting Units")
    @JsonPropertyDescription("Multiple type castings")
    public List<TypeCastingUnit> typeCastingUnits= new ArrayList<>();

    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        Preconditions.checkArgument(!typeCastingUnits.isEmpty());
        return OpExecConfig.oneToOneLayer(operatorIdentifier(),
                OpExecInitInfo.apply((Function<Tuple2<Object, OpExecConfig>, IOperatorExecutor> & java.io.Serializable) worker -> new TypeCastingOpExec(operatorSchemaInfo.outputSchemas()[0])));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Type Casting",
                "Cast between types",
                OperatorGroupConstants.UTILITY_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList(),
                false, false, false, false);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        Schema outputSchema = schemas[0];
        for (TypeCastingUnit unit : typeCastingUnits) {
            outputSchema = AttributeTypeUtils.SchemaCasting(outputSchema, unit.attribute, unit.resultType);
        }
        return outputSchema;
    }
}
