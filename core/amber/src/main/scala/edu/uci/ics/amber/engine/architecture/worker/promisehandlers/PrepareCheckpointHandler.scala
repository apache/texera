package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, WorkflowWorker}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegate
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PrepareCheckpointHandler.PrepareCheckpoint
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.{CheckpointState, CheckpointSupport, SerializedState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

object PrepareCheckpointHandler {
  final case class PrepareCheckpoint(checkpointId:ChannelMarkerIdentity, estimationOnly: Boolean) extends ControlCommand[Unit]
}

trait PrepareCheckpointHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: PrepareCheckpoint, sender) =>
    logger.info("Start to take checkpoint")
    if (!msg.estimationOnly) {
      dp.serializationCall = () => {
        serializeWorkerState()
      }
    } else {
      logger.info(s"Checkpoint is estimation-only. do nothing.")
    }
  }

  private def serializeWorkerState(checkpointId:ChannelMarkerIdentity): Unit = {
    val chkpt = new CheckpointState()
    // 1. serialize DP state
    chkpt.save(SerializedState.DP_STATE_KEY, this.dp)
    // checkpoint itself should not be serialized, thus we register it after serialization
    dp.channelMarkerManager.checkpoints(checkpointId) = chkpt
    logger.info("Serialized DP state")
    // 2. serialize operator state
    if (dp.operatorOpened) {
      dp.operator match {
        case support: CheckpointSupport =>
          dp.outputIterator.setTupleOutput(
            support.serializeState(dp.outputIterator.outputIter, chkpt)
          )
          logger.info("Serialized operator state")
        case _ =>
          logger.info("Operator does not support checkpoint, skip")
      }
    } else {
      logger.info("Operator does not open, nothing to serialize")
    }
    // 3. record inflight messages
    logger.info("Begin collecting inflight messages")
    val waitFuture = new CompletableFuture[Unit]()
    val closure = (worker: WorkflowWorker) => {
      val queuedMsgs = mutable.ArrayBuffer[WorkflowFIFOMessage]()
      worker.inputQueue.forEach {
        case WorkflowWorker.FIFOMessageElement(msg)           => queuedMsgs.append(msg)
        case WorkflowWorker.TimerBasedControlElement(control) => // skip
        case WorkflowWorker.ActorCommandElement(cmd)          => // skip
      }
      chkpt.save(SerializedState.DP_QUEUED_MSG_KEY, queuedMsgs)
      // get all output messages from worker.transferService
      chkpt.save(
        SerializedState.OUTPUT_MSG_KEY,
        worker.transferService.getAllUnAckedMessages.toArray
      )
      logger.info("Main thread: serialized queued and output messages.")
      // start to record input messages on main thread
      worker.inputRecordings(checkpointId) =
        new mutable.ArrayBuffer[WorkflowFIFOMessage]()
      logger.info("Main thread: start recording for input messages from now on.")
      waitFuture.complete(())
      ()
    }
    dp.outputHandler(Left(MainThreadDelegate(closure)))
    waitFuture.get()
  }
}
