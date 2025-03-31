package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OpExecSource
import edu.uci.ics.amber.core.storage.VFSURIFactory
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow._

import java.net.URI

object SpecialPhysicalOpFactory {
  def newSourcePhysicalOp(
      workflowIdentity: WorkflowIdentity,
      executionIdentity: ExecutionIdentity,
      uri: URI,
      downstreamOperator: PhysicalOpIdentity,
      downstreamPort: PortIdentity,
      schema: Schema
  ): PhysicalOp = {

    val (_, _, globalPortIdOption, _) = VFSURIFactory.decodeURI(uri)
    val globalPortId = globalPortIdOption.get
    val outputPort = OutputPort()
    PhysicalOp
      .sourcePhysicalOp(
        PhysicalOpIdentity(
          globalPortId.opId.logicalOpId,
          s"${globalPortId.opId.layerName}_source_${globalPortId.portId.id}_${downstreamOperator.logicalOpId.id
            .replace('-', '_')}_${downstreamPort.id}"
        ),
        workflowIdentity,
        executionIdentity,
        OpExecSource(uri.toString, workflowIdentity)
      )
      .withInputPorts(List.empty)
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(outputPort.id -> schema))
      )
      .propagateSchema()

  }

}
