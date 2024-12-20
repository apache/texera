package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.sink.ProgressiveUtils
import edu.uci.ics.amber.operator.sink.managed.ProgressiveSinkOpExec
import edu.uci.ics.amber.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.workflow.OutputPort.OutputMode.{SET_DELTA, SET_SNAPSHOT, SINGLE_SNAPSHOT}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

object SpecialPhysicalOpFactory {
  def newSinkPhysicalOp(
      workflowIdentity: WorkflowIdentity,
      executionIdentity: ExecutionIdentity,
      storageKey: String,
      outputMode: OutputMode
  ): PhysicalOp =
    PhysicalOp
      .localPhysicalOp(
        PhysicalOpIdentity(OperatorIdentity(storageKey), "sink"),
        workflowIdentity,
        executionIdentity,
        OpExecInitInfo((idx, workers) =>
          new ProgressiveSinkOpExec(
            outputMode,
            storageKey,
            workflowIdentity
          )
        )
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
