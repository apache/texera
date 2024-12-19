package edu.uci.ics.amber.operator.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.core.executor.OpExecInitInfo;
import edu.uci.ics.amber.core.executor.OperatorExecutor;
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils;
import edu.uci.ics.amber.core.tuple.Schema;
import edu.uci.ics.amber.core.workflow.PhysicalOp;
import edu.uci.ics.amber.core.workflow.SchemaPropagationFunc;
import edu.uci.ics.amber.operator.map.MapOpDesc;
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants;
import edu.uci.ics.amber.operator.metadata.OperatorInfo;
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils;
import edu.uci.ics.amber.virtualidentity.ExecutionIdentity;
import edu.uci.ics.amber.virtualidentity.WorkflowIdentity;
import edu.uci.ics.amber.workflow.InputPort;
import edu.uci.ics.amber.workflow.OutputPort;
import edu.uci.ics.amber.workflow.PortIdentity;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static scala.jdk.javaapi.CollectionConverters.asScala;

public class TypeCastingOpDesc extends MapOpDesc {

    @JsonProperty(required = true)
    @JsonSchemaTitle("TypeCasting Units")
    @JsonPropertyDescription("Multiple type castings")
    public java.util.List<edu.uci.ics.amber.operator.typecasting.TypeCastingUnit> typeCastingUnits = new ArrayList<>();

    @Override
    public PhysicalOp getPhysicalOp(WorkflowIdentity workflowId, ExecutionIdentity executionId) {
        if (typeCastingUnits == null) typeCastingUnits = new ArrayList<>();
        return PhysicalOp.oneToOnePhysicalOp(
                        workflowId,
                        executionId,
                        operatorIdentifier(),
                        OpExecInitInfo.apply(
                                (Function<Tuple2<Object, Object>, OperatorExecutor> & java.io.Serializable)
                                        worker -> new edu.uci.ics.amber.operator.typecasting.TypeCastingOpExec(typeCastingUnits)
                        )
                )
                .withInputPorts(operatorInfo().inputPorts())
                .withOutputPorts(operatorInfo().outputPorts())
                .withPropagateSchema(
                        SchemaPropagationFunc.apply((Function<Map<PortIdentity, Schema>, Map<PortIdentity, Schema>> & Serializable) inputSchemas -> {
                            // Initialize a Java HashMap
                            java.util.Map<PortIdentity, Schema> javaMap = new java.util.HashMap<>();
                            Schema outputSchema = inputSchemas.values().head();
                            if (typeCastingUnits != null) {
                                for (edu.uci.ics.amber.operator.typecasting.TypeCastingUnit unit : typeCastingUnits) {
                                    outputSchema = AttributeTypeUtils.SchemaCasting(outputSchema, unit.attribute, unit.resultType);
                                }
                            }

                            javaMap.put(operatorInfo().outputPorts().head().id(), outputSchema);

                            // Convert the Java Map to a Scala immutable Map
                            return OperatorDescriptorUtils.toImmutableMap(javaMap);
                        })
                );
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Type Casting",
                "Cast between types",
                OperatorGroupConstants.CLEANING_GROUP(),
                asScala(singletonList(new InputPort(new PortIdentity(0, false), "", false, asScala(new ArrayList<PortIdentity>()).toSeq()))).toList(),
                asScala(singletonList(new OutputPort(new PortIdentity(0, false), "", false, OutputPort.OutputMode$.MODULE$.fromValue(0)))).toList(),
                false,
                false,
                false,
                false
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Schema outputSchema = schemas[0];
        if (typeCastingUnits != null) {
            for (edu.uci.ics.amber.operator.typecasting.TypeCastingUnit unit : typeCastingUnits) {
                outputSchema = AttributeTypeUtils.SchemaCasting(outputSchema, unit.attribute, unit.resultType);
            }
        }
        return outputSchema;
    }
}
