package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class BatchToTupleConverterSpec extends AnyFlatSpec with MockFactory {
  private val mockInternalQueue = mock[WorkerInternalQueue]
  private val fakeID = WorkerActorVirtualIdentity("testReceiver")
  val linkID1 = LinkIdentity(null, null)
  val linkID2 = LinkIdentity(LayerIdentity("", "", ""), null)
  "tuple producer" should "break batch into tuples and output" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatch = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    inSequence {
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(linkID1))
      inputBatch.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker)
      (mockInternalQueue.appendElement _).expects(EndOfAllMarker)
    }
    batchToTupleConverter.registerInput(fakeID, linkID1)
    batchToTupleConverter.processDataPayload(fakeID, inputBatch)
    batchToTupleConverter.processDataPayload(fakeID, EndOfUpstream())
  }

  "tuple producer" should "be aware of upstream change" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatchFromUpstream1 = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    val inputBatchFromUpstream2 = DataFrame(Array.fill(4)(ITuple(2, 3, 4, 5, "6.7", 8.9)))
    inSequence {
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(linkID1))
      inputBatchFromUpstream1.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(linkID2))
      inputBatchFromUpstream2.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker)
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(linkID1))
      (mockInternalQueue.appendElement _).expects(EndMarker)
      (mockInternalQueue.appendElement _).expects(EndOfAllMarker)
    }
    val first = WorkerActorVirtualIdentity("first upstream")
    val second = WorkerActorVirtualIdentity("second upstream")
    batchToTupleConverter.registerInput(first, linkID1)
    batchToTupleConverter.registerInput(second, linkID2)
    batchToTupleConverter.processDataPayload(first, inputBatchFromUpstream1)
    batchToTupleConverter.processDataPayload(second, inputBatchFromUpstream2)
    batchToTupleConverter.processDataPayload(second, EndOfUpstream())
    batchToTupleConverter.processDataPayload(first, EndOfUpstream())

  }

}
