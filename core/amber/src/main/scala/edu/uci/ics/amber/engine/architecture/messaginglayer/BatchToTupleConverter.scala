package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, VirtualIdentity}

import scala.collection.mutable

class BatchToTupleConverter(workerInternalQueue: WorkerInternalQueue) {

  /**
    * Map from Identifier to input number. Used to convert the Identifier
    * to int when adding sender info to the queue.
    * We also keep track of the upstream actors so that we can emit
    * EndOfAllMarker when all upstream actors complete their job
    */
  private val inputMap = new mutable.HashMap[VirtualIdentity, LinkIdentity]
  private val upstreamMap = new mutable.HashMap[LinkIdentity, mutable.HashSet[VirtualIdentity]]
  private var currentLink: LinkIdentity = _

  def registerInput(identifier: VirtualIdentity, input: LinkIdentity): Unit = {
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
    * @param dataPayload
    */
  def processDataPayload(from: VirtualIdentity, dataPayload: DataPayload): Unit = {
    val link = inputMap(from)
    if (currentLink == null || currentLink != link) {
      workerInternalQueue.appendElement(SenderChangeMarker(link))
      currentLink = link
    }
    dataPayload match {
      case DataFrame(payload) =>
        payload.foreach { i =>
          workerInternalQueue.appendElement(InputTuple(i))
        }
      case EndOfUpstream() =>
        upstreamMap(link).remove(from)
        if (upstreamMap(link).isEmpty) {
          workerInternalQueue.appendElement(EndMarker)
          upstreamMap.remove(link)
        }
        if (upstreamMap.isEmpty) {
          workerInternalQueue.appendElement(EndOfAllMarker)
        }
      case other =>
        throw new NotImplementedError()
    }
  }

}
