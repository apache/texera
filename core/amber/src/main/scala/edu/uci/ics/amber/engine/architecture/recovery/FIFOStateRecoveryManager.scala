package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.ProcessControlMessage
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogReader
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class FIFOStateRecoveryManager(logReader: DeterminantLogReader) {
  private val records = new RecordIterator(logReader)

  def getFIFOState: Map[ActorVirtualIdentity, Long] = {
    val fifoState = new mutable.AnyRefMap[ActorVirtualIdentity, Long]()
    while (!records.isEmpty) {
      records.peek() match {
        case ProcessControlMessage(controlPayload, from) =>
          if (fifoState.contains(from)) {
            fifoState(from) += 1
          } else {
            fifoState(from) = 1
          }
        case other => //skip
      }
      records.readNext()
    }
    fifoState.toMap
  }

}
