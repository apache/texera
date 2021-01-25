package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.ambermessage.neo.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity

import scala.collection.mutable

class BatchToTupleConverter(workerInternalQueue: WorkerInternalQueue) {

  /**
    * Map from Identifier to input number. Used to convert the Identifier
    * to int when adding sender info to the queue.
    * We also keep track of the upstream actors so that we can emit
    * EndOfAllMarker when all upstream actors complete their job
    */
  private val inputMap = new mutable.HashMap[VirtualIdentity, OperatorIdentifier]
  private val upstreamMap =
    new mutable.HashMap[OperatorIdentifier, mutable.HashSet[VirtualIdentity]]
  private var currentSender: OperatorIdentifier = null

  def registerInput(identifier: VirtualIdentity, input: OperatorIdentifier): Unit = {
    upstreamMap.getOrElseUpdate(input, new mutable.HashSet[VirtualIdentity]()).add(identifier)
    inputMap(identifier) = input
  }

  /** This method handles various data payloads and put different
    * element into the internal queue.
    * data payloads:
    * 1. Data Payload, it will be split into tuples and add to the queue.
    * 2. End Of Upstream, this payload will be received once per upstream actor.
    *    Note that multiple upstream actors can be there for one upstream.
    *    We emit EOU marker when one upstream exhausts. Also, we emit End Of All marker
    *    when ALL upstreams exhausts.
    *
    * @param from
    * @param dataPayloads
    */
  def processDataPayload(from: VirtualIdentity, dataPayloads: Iterable[DataPayload]): Unit = {
    val sender = inputMap(from)
    if (currentSender == null || currentSender != sender) {
      workerInternalQueue.appendElement(SenderChangeMarker(sender))
      currentSender = sender
    }
    dataPayloads.foreach {
      case DataFrame(payload) =>
        payload.foreach { i =>
          workerInternalQueue.appendElement(InputTuple(i))
        }
      case EndOfUpstream() =>
        upstreamMap(sender).remove(from)
        if (upstreamMap(sender).isEmpty) {
          workerInternalQueue.appendElement(EndMarker())
          upstreamMap.remove(sender)
        }
        if (upstreamMap.isEmpty) {
          workerInternalQueue.appendElement(EndOfAllMarker())
        }
      case other =>
        throw new NotImplementedError()
    }
  }

}
