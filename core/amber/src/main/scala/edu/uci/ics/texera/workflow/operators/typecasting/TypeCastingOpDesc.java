package edu.uci.ics.texera.workflow.operators.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp;
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo;
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity;
import edu.uci.ics.amber.engine.common.workflow.InputPort;
import edu.uci.ics.amber.engine.common.workflow.OutputPort;
import edu.uci.ics.amber.engine.common.workflow.PortIdentity;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import scala.Tuple3;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class TypeCastingOpDesc extends MapOpDesc {

    @JsonProperty(required = true)
    @JsonSchemaTitle("TypeCasting Units")
    @JsonPropertyDescription("Multiple type castings")
    public java.util.List<TypeCastingUnit> typeCastingUnits = new ArrayList<>();

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        Preconditions.checkArgument(!typeCastingUnits.isEmpty());
        Schema outputSchema = this.outputPortToSchemaMapping().get(this.operatorInfo().outputPorts().head().id()).get();
        return PhysicalOp.oneToOnePhysicalOp(
                        workflowId,
                        executionId,
                        operatorIdentifier(),
                        OpExecInitInfo.apply(
                                (Function<Tuple3<Object, PhysicalOp, OperatorConfig>, IOperatorExecutor> & java.io.Serializable)
                                        worker -> new TypeCastingOpExec(outputSchema)
                        )
                )
                .withInputPorts(operatorInfo().inputPorts(), inputPortToSchemaMapping())
                .withOutputPorts(operatorInfo().outputPorts(), outputPortToSchemaMapping());
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Type Casting",
                "Cast between types",
                OperatorGroupConstants.UTILITY_GROUP(),
                asScalaBuffer(singletonList(new InputPort(new PortIdentity(0, false), "", false, List.empty()))).toList(),
                asScalaBuffer(singletonList(new OutputPort(new PortIdentity(0, false), ""))).toList(),
                false,
                false,
                false,
                false
        );
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
