package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.controlcommands.LinkOrdinal
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LayerIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

object InitializeOperatorLogicHandler {
  final case class InitializeOperatorLogic(
      code: String,
    id: LayerIdentity,
    inputOrdinalMapping: Seq[LinkOrdinal],
    outputOrdinalMapping: Seq[LinkOrdinal],
    isSource: Boolean,
      outputSchema: Schema
  ) extends ControlCommand[Unit]
}
