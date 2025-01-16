package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.{OpExecSink, OpExecSource}
import edu.uci.ics.amber.core.storage.VFSURIFactory
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode.{
  SET_DELTA,
  SET_SNAPSHOT,
  SINGLE_SNAPSHOT
}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.sink.ProgressiveUtils

import java.net.URI

object SpecialPhysicalOpFactory {
  def newSinkPhysicalOp(
      uri: URI,
      outputMode: OutputMode
  ): PhysicalOp = {
    val (workflowIdentity, executionIdentity, opId, portId, _) = VFSURIFactory.decodeURI(uri)
    PhysicalOp
      .localPhysicalOp(
        PhysicalOpIdentity(opId, s"sink${portId.get.id}"),
        workflowIdentity,
        executionIdentity,
        OpExecSink(uri.toString, workflowIdentity, outputMode)
      )
      .withInputPorts(List(InputPort(PortIdentity(internal = true))))
      .withOutputPorts(List(OutputPort(PortIdentity(internal = true))))
      .withPropagateSchema(
        SchemaPropagationFunc((inputSchemas: Map[PortIdentity, Schema]) => {
          // Get the first schema from inputSchemas
          val inputSchema = inputSchemas.values.head

          // Define outputSchema based on outputMode
          val outputSchema = outputMode match {
            case SET_SNAPSHOT | SINGLE_SNAPSHOT =>
              if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
                // with insert/retract delta: remove the flag column
                inputSchema.remove(ProgressiveUtils.insertRetractFlagAttr.getName)
              } else {
                // with insert-only delta: output schema is the same as input schema
                inputSchema
              }

            case SET_DELTA =>
              // output schema is the same as input schema
              inputSchema
            case _ =>
              throw new UnsupportedOperationException(s"Output mode $outputMode is not supported.")
          }

          // Create a Scala immutable Map
          Map(PortIdentity(internal = true) -> outputSchema)
        })
      )
  }

  def newSourcePhysicalOp(
      workflowIdentity: WorkflowIdentity,
      executionIdentity: ExecutionIdentity,
      uri: URI
  ): PhysicalOp = {

    val (_, _, opId, portId, _) = VFSURIFactory.decodeURI(uri)
    val outputPort = OutputPort()
    PhysicalOp
      .sourcePhysicalOp(
        PhysicalOpIdentity(opId, s"source${portId.get.id}"),
        workflowIdentity,
        executionIdentity,
        OpExecSource(uri.toString, workflowIdentity)
      )
      .withInputPorts(List.empty)
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(_ =>
          Map(outputPort.id -> {
            DocumentFactory.openDocument(uri)._2.get
          })
        )
      )
      .propagateSchema()

  }

}
