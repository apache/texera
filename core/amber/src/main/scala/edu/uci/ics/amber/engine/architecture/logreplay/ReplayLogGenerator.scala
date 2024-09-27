package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity

import scala.collection.mutable

object ReplayLogGenerator {
  def generate(
      logStorage: SequentialRecordStorage[ReplayLogRecord],
      logFileName: String,
      replayTo: ChannelMarkerIdentity
  ): (mutable.Queue[ProcessingStep], mutable.Queue[ProcessingStep], mutable.Queue[WorkflowFIFOMessage], Option[ChannelMarkerIdentity]) = {
    val additionalSteps = mutable.Queue[ProcessingStep]()
    val logs = logStorage.getReader(logFileName).mkRecordIterator()
    val steps = mutable.Queue[ProcessingStep]()
    val messages = mutable.Queue[WorkflowFIFOMessage]()
    var lastCheckpoint: Option[ChannelMarkerIdentity] = None
    var stage2 = false
    logs.foreach {
      case s: ProcessingStep =>
        if(stage2){
          additionalSteps.enqueue(s)
        }else{
          steps.enqueue(s)
        }
      case MessageContent(message) =>
        messages.enqueue(message)
      case ReplayDestination(id) =>
        if (id == replayTo) {
          // we only need log record upto this point
          stage2 = true
        }else{
          if(!stage2 && id.id.contains("Checkpoint")){
            lastCheckpoint = Some(id)
            steps.clear()
            messages.clear()
          }
        }
      case other =>
        throw new RuntimeException(s"cannot handle $other in the log")
    }
    (steps, additionalSteps, messages, lastCheckpoint)
  }
}
