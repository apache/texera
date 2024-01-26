package edu.uci.ics.texera.workflow.operators.dummy

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo

import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{
  OperatorDescriptor,
  PortDescriptor,
  StateTransferFunc
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import scala.util.{Success, Try}


class DummyOpDesc extends OperatorDescriptor with PortDescriptor {

  @JsonProperty
  @JsonSchemaTitle("Description")
  @JsonPropertyDescription("The description of this dummy operator")
  var desc: String = ""

  //override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
  //  OpExecConfig.oneToOneLayer(operatorIdentifier, _ => new DummyOpExec())
  //}

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig.oneToOneLayer(operatorIdentifier, OpExecInitInfo(_ => new DummyOpExec()))
  }

  override def operatorInfo: OperatorInfo = {

    val inputPortInfo = if (inputPorts != null) {
      inputPorts.map(p => InputPort(p.displayName, p.allowMultiInputs))
    } else {
      List(InputPort("", allowMultiInputs = true))
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.map(p => OutputPort(p.displayName))
    } else {
      List(OutputPort(""))
    }

    OperatorInfo(
      "Dummy",
      "a dummy operator used as a placeholder",
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

  override def runtimeReconfiguration(
      newOpDesc: OperatorDescriptor,
      operatorSchemaInfo: OperatorSchemaInfo
  ): Try[(OpExecConfig, Option[StateTransferFunc])] = {
    Success(newOpDesc.operatorExecutor(operatorSchemaInfo), None)
  }
}
