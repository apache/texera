package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{EndMarker, InputTuple}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BatchToTupleConverter(
    workerInternalQueue: WorkerInternalQueue
) {

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
    dataPayload match {
      case DataFrame(payload) =>
        payload.foreach { i =>
          workerInternalQueue.appendElement(InputTuple(from, i))
        }
      case EndOfUpstream() =>
        workerInternalQueue.appendElement(EndMarker(from))
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
