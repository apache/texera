package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage

import scala.collection.mutable

object ReplayLogGenerator {
  def generate(
      logStorage: ReplayLogStorage,
      logFileName: String
  ): (mutable.Queue[ProcessingStep], mutable.Queue[WorkflowFIFOMessage]) = {
    val logs = logStorage.getReader(logFileName).mkLogRecordIterator()
    val steps = mutable.Queue[ProcessingStep]()
    val messages = mutable.Queue[WorkflowFIFOMessage]()
    logs.foreach {
      case s: ProcessingStep =>
        steps.enqueue(s)
      case MessageContent(message) =>
        messages.enqueue(message)
      case other =>
        throw new RuntimeException(s"cannot handle $other in the log")
    }
    (steps, messages)
  }
}
