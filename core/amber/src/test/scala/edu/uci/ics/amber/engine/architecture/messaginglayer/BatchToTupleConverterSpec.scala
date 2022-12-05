package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.logging.storage.EmptyLogStorage
import edu.uci.ics.amber.engine.architecture.logging.{
  DeterminantLogger,
  EmptyLogManagerImpl,
  LogManager
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.recovery.{LocalRecoveryManager, RecoveryQueue}
import edu.uci.ics.amber.engine.architecture.worker.{PauseManager, WorkerInternalQueue}
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{EndMarker, InputTuple}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class BatchToTupleConverterSpec extends AnyFlatSpec with MockFactory {
  val linkID1: LinkIdentity = LinkIdentity(null, null)
  val linkID2: LinkIdentity = LinkIdentity(LayerIdentity("", "", ""), null)

  class TestWorkerInternalQueue extends WorkerInternalQueue {
    val pauseManager = new PauseManager()
    val logManager = new EmptyLogManagerImpl(NetworkSenderActorRef(null))
    val recoveryQueue = new RecoveryQueue(new EmptyLogStorage().getReader)
    val recoveryManager: LocalRecoveryManager = new LocalRecoveryManager()
  }

  private val mockInternalQueue = mock[TestWorkerInternalQueue]
  private val fakeID = ActorVirtualIdentity("testReceiver")

  "tuple producer" should "break batch into tuples and output" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatch = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    val senderID = ActorVirtualIdentity("testSender")
    inSequence {
      inputBatch.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(senderID, i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker(senderID))
    }
    batchToTupleConverter.processDataPayload(senderID, inputBatch)
    batchToTupleConverter.processDataPayload(senderID, EndOfUpstream())
  }

  "tuple producer" should "be aware of upstream change" in {
    val batchToTupleConverter = wire[BatchToTupleConverter]
    val inputBatchFromUpstream1 = DataFrame(Array.fill(4)(ITuple(1, 2, 3, 5, "9.8", 7.6)))
    val inputBatchFromUpstream2 = DataFrame(Array.fill(4)(ITuple(2, 3, 4, 5, "6.7", 8.9)))
    val first = ActorVirtualIdentity("first upstream")
    val second = ActorVirtualIdentity("second upstream")
    inSequence {
      inputBatchFromUpstream1.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(first, i))
      }
      inputBatchFromUpstream2.frame.foreach { i =>
        (mockInternalQueue.appendElement _).expects(InputTuple(second, i))
      }
      (mockInternalQueue.appendElement _).expects(EndMarker(second))
      (mockInternalQueue.appendElement _).expects(EndMarker(first))
    }
    batchToTupleConverter.processDataPayload(first, inputBatchFromUpstream1)
    batchToTupleConverter.processDataPayload(second, inputBatchFromUpstream2)
    batchToTupleConverter.processDataPayload(second, EndOfUpstream())
    batchToTupleConverter.processDataPayload(first, EndOfUpstream())

  }

}
