package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptImmutableStateHandler.AcceptImmutableState
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SaveSkewedWorkerInfoHandler.SaveSkewedWorkerInfo
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendImmutableStateOrNotifyHelperHandler.SendImmutableStateOrNotifyHelper
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec
import edu.uci.ics.texera.workflow.operators.sortPartitions.SortPartitionOpExec

import scala.collection.mutable.ArrayBuffer

/**
  * This handler is used to do state migration during Reshape for immutable state operator
  * like HashJoin.
  * e.g., The controller will send a `SendImmutableState` message to
  * a skewed worker of HashJoin operator to send its build hash map
  * to `helperReceiverId` worker.
  *
  * For mutable state operators such as sort, it just notifies the skewed worker about its
  * helper. The skewed worker saves this info and later uses this to ask for final results
  * from the helper.
  *
  * Possible sender: Controller (SkewDetectionHandler).
  */
object SendImmutableStateOrNotifyHelperHandler {
  final case class SendImmutableStateOrNotifyHelper(
      helperReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Boolean]
}

trait SendImmutableStateOrNotifyHelperHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: SendImmutableStateOrNotifyHelper, sender) =>
    if (dataProcessor.getOperatorExecutor().isInstanceOf[HashJoinOpExec[Any]]) {
      // Returns true if the build table was replicated successfully in case of HashJoin.
      try {
        val joinOpExec = dataProcessor.getOperatorExecutor().asInstanceOf[HashJoinOpExec[Any]]
        if (joinOpExec.isBuildTableFinished) {
          val immutableStates = joinOpExec.getBuildHashTableBatches()
          val immutableStatesSendingFutures = new ArrayBuffer[Future[Boolean]]()
          immutableStates.foreach(map => {
            immutableStatesSendingFutures.append(
              send(AcceptImmutableState(map), cmd.helperReceiverId)
            )
          })
          Future
            .collect(immutableStatesSendingFutures)
            .flatMap(seq => {
              if (!seq.contains(false)) {
                logger.info(
                  s"Reshape: Replication of all parts of build table done to ${cmd.helperReceiverId}"
                )
                Future.True
              } else {
                Future.False
              }
            })
        } else {
          Future.False
        }
      } catch {
        case exception: Exception =>
          logger.error(
            "Reshape: SendImmutableStateHandler exception" + exception
              .getMessage() + " stacktrace " + exception.getStackTrace()
          )
          Future.False
      }
    } else if (dataProcessor.getOperatorExecutor().isInstanceOf[SortPartitionOpExec]) {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[SortPartitionOpExec]
        .waitingForTuplesFromHelper = true
      dataProcessor.getOperatorExecutor().asInstanceOf[SortPartitionOpExec].helperWorkerIdentity =
        cmd.helperReceiverId
      send(SaveSkewedWorkerInfo(actorId), cmd.helperReceiverId).map(response => response)
    } else {
      Future.False
    }
  }
}
