package edu.uci.ics.texera.workflow.operators.split

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.util.Random

class SplitOpDesc extends LogicalOp {

  @JsonProperty(value = "training percentage", required = false, defaultValue = "80")
  @JsonPropertyDescription("percentage of training split data (default 80%)")
  var k: Int = 80

  // Store random seeds for each executor to satisfy the fault tolerance requirement.
  @JsonIgnore
  val seeds: Array[Int] = Array.fill(AmberConfig.numWorkerPerOperatorByDefault)(Random.nextInt)

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((idx, _, _) => new SplitOpExec(idx, this))
      )
      .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
      .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Training/Testing Split",
      operatorDescription = "Split training and testing data to two different ports",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(
        OutputPort(PortIdentity(), displayName = "training"),
        OutputPort(PortIdentity(1), displayName = "testing")
      ),
      dynamicInputPorts = true,
      dynamicOutputPorts = true
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = throw new NotImplementedError()

  override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] = {
    Preconditions.checkArgument(schemas.length == 1)
    Array(schemas(0), schemas(0))
  }
}
