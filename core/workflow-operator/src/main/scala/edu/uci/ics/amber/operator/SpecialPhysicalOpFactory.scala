package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.sink.ProgressiveUtils
import edu.uci.ics.amber.operator.sink.managed.ProgressiveSinkOpExec
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.workflow.OutputPort.OutputMode.SET_SNAPSHOT
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

object SpecialPhysicalOpFactory {
  def newSinkPhysicalOp(workflowIdentity: WorkflowIdentity, executionIdentity: ExecutionIdentity, storageKey: String, outputMode: OutputMode, isMaterialization: Boolean = false): PhysicalOp = PhysicalOp.localPhysicalOp(
      workflowIdentity,
      executionIdentity,
      OperatorIdentity("sink_" + storageKey),
      OpExecInitInfo(
        (idx, workers) =>
          new ProgressiveSinkOpExec(
            outputMode,
            storageKey,
            workflowIdentity
          )
      )
    )
    .withInputPorts(List(InputPort(PortIdentity())))
    .withOutputPorts(List(OutputPort(PortIdentity())))
    .withPropagateSchema(
      SchemaPropagationFunc((inputSchemas: Map[PortIdentity, Schema]) => {
        // Get the first schema from inputSchemas
        val inputSchema = inputSchemas.values.head

        // Define outputSchema based on outputMode
        val outputSchema = if (outputMode == SET_SNAPSHOT) {
          if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
            // input is insert/retract delta: remove the flag column in the output
            Schema.builder()
              .add(inputSchema)
              .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
              .build()
          } else {
            // input is insert-only delta: output schema is the same as input schema
            inputSchema
          }
        } else {
          // SET_DELTA: output schema is the same as input schema
          inputSchema
        }

        // Create a Scala immutable Map
        Map(PortIdentity() -> outputSchema)
      })
    )
}
