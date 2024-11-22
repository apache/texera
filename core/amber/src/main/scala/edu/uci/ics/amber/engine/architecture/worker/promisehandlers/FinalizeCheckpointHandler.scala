package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  FinalizeCheckpointRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.FinalizeCheckpointResponse
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.worker.{
  DataProcessorRPCHandlerInitializer,
  WorkflowWorker
}
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.{CheckpointState, CheckpointSupport, SerializedState}

import java.net.URI
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer

trait FinalizeCheckpointHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def finalizeCheckpoint(
      msg: FinalizeCheckpointRequest,
      ctx: AsyncRPCContext
  ): Future[FinalizeCheckpointResponse] = {
    val checkpointSize = if (dp.channelMarkerManager.checkpoints.contains(msg.checkpointId)) {
      val waitFuture = new CompletableFuture[Unit]()
      val chkpt = dp.channelMarkerManager.checkpoints(msg.checkpointId)
      val closure = (worker: WorkflowWorker) => {
        logger.info(s"Main thread: start to serialize recorded messages.")
        chkpt.save(
          SerializedState.IN_FLIGHT_MSG_KEY,
          worker.recordedInputs.getOrElse(msg.checkpointId, new ArrayBuffer())
        )
        worker.recordedInputs.remove(msg.checkpointId)
        logger.info(s"Main thread: recorded messages serialized.")
        waitFuture.complete(())
        ()
      }
      // TODO: find a way to skip logging for the following output?
      dp.outputHandler(
        Left(MainThreadDelegateMessage(closure))
      ) //this will create duplicate log records!
      waitFuture.get()
      logger.info(s"Start to write checkpoint to storage. Destination: ${msg.writeTo}")
      val storage = SequentialRecordStorage.getStorage[CheckpointState](Some(new URI(msg.writeTo)))
      val writer = storage.getWriter(actorId.name.replace("Worker:", ""))
      writer.writeRecord(chkpt)
      writer.flush()
      writer.close()
      logger.info(s"Checkpoint finalized, total size = ${chkpt.size()} bytes")
      chkpt.size()
    } else {
      logger.info(s"Checkpoint is estimation-only. report estimated size.")
      dp.executor match {
        case support: CheckpointSupport =>
          support.getEstimatedCheckpointCost
        case _ => 0L
      } // for estimation
    }
    FinalizeCheckpointResponse(checkpointSize)
  }
}
