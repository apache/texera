package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.OneToOnePolicy
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class TupleToBatchConverterSpec extends AnyFlatSpec with MockFactory {
  private val mockDataOutputPort = mock[DataOutputPort]
  private val identifier = ActorVirtualIdentity("batch producer mock")

  "TupleToBatchConverter" should "aggregate tuples and output" in {
    val batchProducer = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    val fakeID = ActorVirtualIdentity("testReceiver")
    inSequence {
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(0, 10)))
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(10, 20)))
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(20, 21)))
      (mockDataOutputPort.sendTo _).expects(fakeID, EndOfUpstream())
    }
    val fakeLink =
      LinkIdentity(Option(LayerIdentity("", "", "")), Option(LayerIdentity("", "", "")))
    val fakeReceiver = Array[ActorVirtualIdentity](fakeID)

    batchProducer.addPolicy(new OneToOnePolicy(fakeLink, 10, fakeReceiver))
    tuples.foreach { t =>
      batchProducer.passTupleToDownstream(t)
    }
    batchProducer.emitEndOfUpstream()
  }

  "TupleToBatchConverter" should "not output tuples when there is no policy" in {
    val tupleToBatchConverter = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    (mockDataOutputPort.sendTo _).expects(*, *).never()
    tuples.foreach { t =>
      tupleToBatchConverter.passTupleToDownstream(t)
    }
    tupleToBatchConverter.emitEndOfUpstream()
  }

}
