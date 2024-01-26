package edu.uci.ics.texera.workflow.operators.dummy

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp.oneToOnePhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PortDescription
import edu.uci.ics.texera.workflow.common.workflow.PartitionInfo
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.operators.{LogicalOp, PortDescriptor, StateTransferFunc}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.workflow.operators.projection.ProjectionOpExec


class DummyOpDesc extends LogicalOp with PortDescriptor {

  @JsonProperty
  @JsonSchemaTitle("Description")
  @JsonPropertyDescription("The description of this dummy operator")
  var desc: String = ""

  //override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
  //  OpExecConfig.oneToOneLayer(operatorIdentifier, OpExecInitInfo(_ => new DummyOpExec()))
  //}

  override def operatorInfo: OperatorInfo = {
    val inputPortInfo = if (inputPorts != null) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            allowMultiLinks = portDesc.allowMultiInputs,
            dependencies = portDesc.dependencies.map(idx => PortIdentity(idx))
          )
      }
    } else {
      List(InputPort(PortIdentity(), allowMultiLinks = true))
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) => OutputPort(PortIdentity(idx), displayName = portDesc.displayName)
      }
    } else {
      List(OutputPort())
    }

    OperatorInfo(
      "Dummy",
      "A dummy operator used as a placeholder.",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPortInfo,
      outputPortInfo,
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)

  //val workflowInstance = WorkflowWithPrivilege(


  //)

  //override def runtimeReconfiguration(
  //    newOpDesc: LogicalOp,
  //    operatorSchemaInfo: OperatorSchemaInfo
  //): Try[(OpExecConfig, Option[StateTransferFunc])] = {
  //  Success(newOpDesc.operatorExecutor(operatorSchemaInfo), None)
 // }
}
