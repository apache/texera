package edu.uci.ics.amber.engine.architecture.deploysemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.LocationPreference
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.PropertyNameConstants
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.PartitionInfo
import org.codehaus.jackson.annotate.JsonProperty

object PhysicalOpPojo {
  def apply(physicalOp: PhysicalOp): PhysicalOpPojo = {

    val result = new PhysicalOpPojo()
    result.id = physicalOp.id
    result.workflowId = physicalOp.workflowId
    result.executionId = physicalOp.executionId
    result.parallelizable = physicalOp.parallelizable
    result.locationPreference = physicalOp.locationPreference
    result.partitionRequirement = physicalOp.partitionRequirement
    result.inputPorts = physicalOp.inputPorts
    result.outputPorts = physicalOp.outputPorts
    result.isOneToManyOp = physicalOp.isOneToManyOp
    result.suggestedWorkerNum = physicalOp.suggestedWorkerNum

    result
  }
}

class PhysicalOpPojo extends Serializable {

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var id: PhysicalOpIdentity = _

  @JsonProperty(PropertyNameConstants.WORKFLOW_ID)
  var workflowId: WorkflowIdentity = _

  @JsonProperty(PropertyNameConstants.EXECUTION_ID)
  var executionId: ExecutionIdentity = _

  @JsonProperty(PropertyNameConstants.PARALLELIZABLE)
  var parallelizable: Boolean = _

  @JsonProperty(PropertyNameConstants.LOCATION_PREFERENCE)
  var locationPreference: Option[LocationPreference] = _

  @JsonProperty(PropertyNameConstants.PARTITION_REQUIREMENT)
  var partitionRequirement: List[Option[PartitionInfo]] = _

  @JsonProperty(PropertyNameConstants.INPUT_PORTS)
  var inputPorts: Map[PortIdentity, (InputPort, List[PhysicalLink], Either[Throwable, Schema])] = _

  @JsonProperty(PropertyNameConstants.OUTPUT_PORTS)
  var outputPorts: Map[PortIdentity, (OutputPort, List[PhysicalLink], Either[Throwable, Schema])] =
    _

  @JsonProperty(PropertyNameConstants.IS_ONE_TO_MANY_OP)
  var isOneToManyOp: Boolean = _

  @JsonProperty(PropertyNameConstants.SUGGESTED_WORKER_NUM)
  var suggestedWorkerNum: Option[Int] = _
}
