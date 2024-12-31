package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.{OpExecSink, OpExecSource}
import edu.uci.ics.amber.core.storage.result.{OpResultStorage, ResultStorage}
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

object SpecialPhysicalOpFactory {
  def newSinkPhysicalOp(
      workflowIdentity: WorkflowIdentity,
      executionIdentity: ExecutionIdentity,
      storageKey: String,
      outputMode: OutputMode
  ): PhysicalOp = {
    val (opId, portId) = OpResultStorage.decodeStorageKey(storageKey)
    PhysicalOp
      .localPhysicalOp(
        PhysicalOpIdentity(opId, s"sink${portId.id}"),
        workflowIdentity,
        executionIdentity,
        OpExecSink(storageKey, workflowIdentity, outputMode)
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
                Schema
                  .builder()
                  .add(inputSchema)
                  .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
                  .build()
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
      storageKey: String
  ): PhysicalOp = {

    val (opId, portId) = OpResultStorage.decodeStorageKey(storageKey)
    val opResultStorage = ResultStorage.getOpResultStorage(workflowIdentity)
    val outputPort = OutputPort()
    PhysicalOp
      .sourcePhysicalOp(
        PhysicalOpIdentity(opId, s"source${portId.id}"),
        workflowIdentity,
        executionIdentity,
        OpExecSource(storageKey, workflowIdentity)
      )
      .withInputPorts(List.empty)
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(outputPort.id -> opResultStorage.getSchema(storageKey)))
      )
      .propagateSchema()

  }

}
