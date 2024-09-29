package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, WorkflowWorker}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.FinalizeCheckpointHandler.FinalizeCheckpoint
import edu.uci.ics.amber.engine.common.{AmberConfig, CheckpointState, CheckpointSupport, SerializedState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity

import java.net.URI
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer

object FinalizeCheckpointHandler {
  final case class FinalizeCheckpoint(checkpointId: ChannelMarkerIdentity, writeTo: URI)
      extends ControlCommand[Long]
}

trait FinalizeCheckpointHandler {
  this: DataProcessorRPCHandlerInitializer =>

  private def createArrayOfSize(n: Long): Array[Byte] = {
    // Calculate the size in bytes
    val sizeInBytes = n * 1024

    // Create an array of bytes with the required size
    val array = new Array[Byte](sizeInBytes.toInt)

    // Optionally, you can fill the array with some values if needed
    // scala.util.Random.nextBytes(array)

    // Return the array
    array
  }

  registerHandler { (msg: FinalizeCheckpoint, sender) =>
    if (dp.channelMarkerManager.checkpoints.contains(msg.checkpointId)) {
      val waitFuture = new CompletableFuture[Unit]()
      val chkpt = dp.channelMarkerManager.checkpoints(msg.checkpointId)
      dp.channelMarkerManager.checkpoints.remove(msg.checkpointId)
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
      val storage = SequentialRecordStorage.getStorage[CheckpointState](Some(msg.writeTo))
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
  }
}
