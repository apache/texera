package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable

class BatchToTupleConverter(
    workerInternalQueue: WorkerInternalQueue,
    allUpstreamLinkIds: Set[LinkIdentity]
) {

  /**
    * Map from Identifier to input number. Used to convert the Identifier
    * to int when adding sender info to the queue.
    * We also keep track of the upstream actors so that we can emit
    * EndOfAllMarker when all upstream actors complete their job
    */
  private val inputMap = new mutable.HashMap[ActorVirtualIdentity, LinkIdentity]
  private var currentLink: LinkIdentity = _

  /**
    * The scheduler may not schedule the entire workflow at once. Consider a 2-phase hash join where the first
    * region to be scheduled is the build part of the workflow and the join operator. The hash join workers will
    * only receive the workers from the upstream operator on the build side in `upstreamMap` through
    * `UpdateInputLinkingHandler`. Thus, the hash join worker may wrongly deduce that all inputs are done when
    * the build part completes. Therefore, we have a `allUpstreamLinkIds` to track the number of actual upstream
    * links that a worker receives data from.
    */
  private val upstreamMap = new mutable.HashMap[LinkIdentity, mutable.HashSet[ActorVirtualIdentity]]
  private val endReceivedFromWorkers =
    new mutable.HashMap[LinkIdentity, mutable.HashSet[ActorVirtualIdentity]]
  private val completedLinkIds = new mutable.HashSet[LinkIdentity]()

  def registerInput(identifier: ActorVirtualIdentity, input: LinkIdentity): Unit = {
    upstreamMap.getOrElseUpdate(input, new mutable.HashSet[ActorVirtualIdentity]()).add(identifier)
    inputMap(identifier) = input
  }

  /** This method handles various data payloads and put different
    * element into the internal queue.
    * data payloads:
    * 1. Data Payload, it will be split into tuples and add to the queue.
    * 2. End Of Upstream, this payload will be received once per upstream actor.
    * Note that multiple upstream actors can be there for one upstream.
    * We emit EOU marker when one upstream exhausts. Also, we emit End Of All marker
    * when ALL upstreams exhausts.
    *
    * @param from
    * @param dataPayload
    */
  def processDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    val link = inputMap(from)
    if (currentLink == null || currentLink != link) {
      workerInternalQueue.appendElement(SenderChangeMarker(link))
      currentLink = link
    }
    dataPayload match {
      case DataFrame(payload) =>
        payload.foreach { i =>
          workerInternalQueue.appendElement(InputTuple(from, i))
        }
      case EndOfUpstream() =>
        val existingEndReceived =
          endReceivedFromWorkers.getOrElseUpdate(link, new mutable.HashSet[ActorVirtualIdentity]())
        existingEndReceived.add(from)
        if (upstreamMap(link).equals(endReceivedFromWorkers(link))) {
          completedLinkIds.add(link)
          workerInternalQueue.appendElement(EndMarker)
        }
        if (completedLinkIds.equals(allUpstreamLinkIds)) {
          workerInternalQueue.appendElement(EndOfAllMarker)
        }
      case other =>
        throw new NotImplementedError()
    }
  }

  /**
    * This method is used by flow control logic. It returns the number of credits available for this particular sender
    * worker.
    * @param sender the worker sending the network message
    * @return
    */
  def getSenderCredits(sender: ActorVirtualIdentity): Int =
    workerInternalQueue.getSenderCredits(sender)

}
