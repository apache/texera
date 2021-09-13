package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ReplayCurrentTupleHandler {
  final case class ReplayCurrentTuple() extends ControlCommand[Unit]
}
