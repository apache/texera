package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class TupleToBatchConverterSpec extends AnyFlatSpec with MockFactory {
  private val mockHandler =
    mock[(ActorVirtualIdentity, ActorVirtualIdentity, Long, DataPayload) => Unit]
  private val identifier = ActorVirtualIdentity("batch producer mock")
  private val mockDataOutputPort: NetworkOutputPort[DataPayload] =
    new NetworkOutputPort[DataPayload](identifier, mockHandler)
  var counter: Int = 0

  def layerID(): LayerIdentity = {
    counter += 1
    LayerIdentity("" + counter, "" + counter, "" + counter)
  }

  "TupleToBatchConverter" should "aggregate tuples and output" in {
    val batchProducer = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    val fakeID = ActorVirtualIdentity("testReceiver")
    inSequence {
      (mockHandler.apply _).expects(fakeID, identifier, 0, DataFrame(tuples.slice(0, 10)))
      (mockHandler.apply _).expects(fakeID, identifier, 1, DataFrame(tuples.slice(10, 20)))
      (mockHandler.apply _).expects(fakeID, identifier, 2, DataFrame(tuples.slice(20, 21)))
      (mockHandler.apply _).expects(fakeID, identifier, 3, EndOfUpstream())
    }
    val fakeLink =
      LinkIdentity(layerID(), layerID())
    val fakeReceiver = Array[ActorVirtualIdentity](fakeID)

    batchProducer.addPartitionerWithPartitioning(fakeLink, OneToOnePartitioning(10, fakeReceiver))
    tuples.foreach { t =>
      batchProducer.passTupleToDownstream(t)
    }
    batchProducer.emitEndOfUpstream()
  }

  "TupleToBatchConverter" should "not output tuples when there is no partitioning" in {
    val tupleToBatchConverter = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    (mockHandler.apply _).expects(*, *, *, *).never()
    tuples.foreach { t =>
      tupleToBatchConverter.passTupleToDownstream(t)
    }
    tupleToBatchConverter.emitEndOfUpstream()
  }

}
