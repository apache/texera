package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.common.ambermessage.neo.{DataPayload, WorkflowMessage}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity

import scala.collection.mutable

object DataInputPort {
  final case class WorkflowDataMessage(
      from: VirtualIdentity,
      sequenceNumber: Long,
      payload: DataPayload
  ) extends WorkflowMessage
}

class DataInputPort(tupleProducer: BatchToTupleConverter) {
  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[DataPayload]]()

  def handleDataMessage(msg: WorkflowDataMessage): Unit = {
    OrderingEnforcer.reorderMessage(
      idToOrderingEnforcers,
      msg.from,
      msg.sequenceNumber,
      msg.payload
    ) match {
      case Some(iterable) =>
        tupleProducer.processDataPayload(msg.from, iterable)
      case None =>
      // discard duplicate
    }
  }

  // join-skew research related
  def getStashedMessageCount(): Long = {
    if (idToOrderingEnforcers.size == 0) { return 0 }
    idToOrderingEnforcers.values.map(ordEnforcer => ordEnforcer.ofoMap.size).sum
  }

}
