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
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class BatchToTupleConverterSpec extends AnyFlatSpec with MockFactory {
  private val mockInternalQueue = mock[WorkerInternalQueue]
  private val fakeID = WorkerActorVirtualIdentity("testReceiver")

  "tuple producer" should "break batch into tuples and output" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatch = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    inSequence {
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(0))
      inputBatch.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker())
      (mockInternalQueue.appendElement _).expects(EndOfAllMarker())
    }
    batchToTupleConverter.registerInput(fakeID, 0)
    batchToTupleConverter.processDataPayload(fakeID, Iterable(inputBatch))
    batchToTupleConverter.processDataPayload(fakeID, Iterable(EndOfUpstream()))
  }

  "tuple producer" should "be aware of upstream change" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatchFromUpstream1 = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    val inputBatchFromUpstream2 = DataFrame(Array.fill(4)(ITuple(2, 3, 4, 5, "6.7", 8.9)))
    inSequence {
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(0))
      inputBatchFromUpstream1.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(1))
      inputBatchFromUpstream2.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker())
      (mockInternalQueue.appendElement _).expects(SenderChangeMarker(0))
      (mockInternalQueue.appendElement _).expects(EndMarker())
      (mockInternalQueue.appendElement _).expects(EndOfAllMarker())
    }
    val first = WorkerActorVirtualIdentity("first upstream")
    val second = WorkerActorVirtualIdentity("second upstream")
    batchToTupleConverter.registerInput(first, 0)
    batchToTupleConverter.registerInput(second, 1)
    batchToTupleConverter.processDataPayload(first, Iterable(inputBatchFromUpstream1))
    batchToTupleConverter.processDataPayload(
      second,
      Iterable(inputBatchFromUpstream2, EndOfUpstream())
    )
    batchToTupleConverter.processDataPayload(first, Iterable(EndOfUpstream()))

  }

}
