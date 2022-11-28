package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptImmutableStateHandler.AcceptImmutableState
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * This handler is used to receive the state migrated during Reshape.
  * e.g., A skewed worker of HashJoin will send an `AcceptImmutableState`
  * message to helper worker and the build hash table will be in the
  * message.
  *
  * Possible sender: Skewed worker of the same operator as this worker.
  * (SendImmutableStateHandler)
  */
object AcceptImmutableStateHandler {
  final case class AcceptImmutableState(
      buildHashMap: mutable.HashMap[_, ArrayBuffer[Tuple]]
  ) extends ControlCommand[Boolean]
}

trait AcceptImmutableStateHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: AcceptImmutableState, sender) =>
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[HashJoinOpExec[_]]
        .mergeIntoHashTable(cmd.buildHashMap)
    } catch {
      case exception: Exception =>
        logger.error("Reshape: ", exception)
        false
    }
  }
}
