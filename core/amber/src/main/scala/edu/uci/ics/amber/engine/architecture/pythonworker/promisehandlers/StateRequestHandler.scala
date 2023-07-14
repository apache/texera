package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.controlreturns.StateReturn
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object StateRequestHandler {
  final case class StateRequest(tupleId: String, lineNo: Int, stateName: String)
      extends ControlCommand[StateReturn]
}
