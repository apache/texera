package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptImmutableStateHandler.AcceptImmutableState
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AcceptImmutableStateHandler {
  final case class AcceptImmutableState(
      buildHashMap: mutable.HashMap[Any, ArrayBuffer[Tuple]]
  ) extends ControlCommand[Unit]
}

trait AcceptImmutableStateHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: AcceptImmutableState, sender) =>
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[HashJoinOpExec[Any]]
        .mergeIntoHashTable(cmd.buildHashMap)
    } catch {
      case exception: Exception =>
        logger.error(
          "Reshape: AcceptImmutableStateHandler exception" + exception
            .getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
