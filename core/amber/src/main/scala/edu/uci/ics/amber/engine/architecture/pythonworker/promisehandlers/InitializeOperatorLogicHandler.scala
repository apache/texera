package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
object InitializeOperatorLogicHandler {
  final case class InitializeOperatorLogic(
      code: String,
      allUpstreamLinkIds: Array[LinkIdentity],
      isSource: Boolean,
      outputSchema: Schema
  ) extends ControlCommand[Unit]
}
