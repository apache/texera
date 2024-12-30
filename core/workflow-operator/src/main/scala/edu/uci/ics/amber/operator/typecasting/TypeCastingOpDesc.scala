package edu.uci.ics.amber.operator.typecasting

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class TypeCastingOpDesc extends MapOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("TypeCasting Units")
  @JsonPropertyDescription("Multiple type castings")
  var typeCastingUnits: List[TypeCastingUnit] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    if (typeCastingUnits == null) typeCastingUnits = List.empty
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new TypeCastingOpExec(typeCastingUnits))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc { inputSchemas: Map[PortIdentity, Schema] =>
          val outputSchema = typeCastingUnits.foldLeft(inputSchemas.values.head) { (schema, unit) =>
            AttributeTypeUtils.SchemaCasting(schema, unit.attribute, unit.resultType)
          }
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        }
      )
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Type Casting",
      "Cast between types",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    typeCastingUnits.foldLeft(schemas.head) { (schema, unit) =>
      AttributeTypeUtils.SchemaCasting(schema, unit.attribute, unit.resultType)
    }
  }
}
