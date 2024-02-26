package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class OutputManagerSpec extends AnyFlatSpec with MockFactory {
  private val mockHandler =
    mock[WorkflowFIFOMessage => Unit]
  private val identifier = ActorVirtualIdentity("batch producer mock")
  private val mockDataOutputPort = // scalafix:ok; need it for wiring purpose
    new NetworkOutputGateway(identifier, mockHandler)
  var counter: Int = 0
  val schema: Schema = Schema
    .newBuilder()
    .add("field1", AttributeType.INTEGER)
    .add("field2", AttributeType.INTEGER)
    .add("field3", AttributeType.INTEGER)
    .add("field4", AttributeType.INTEGER)
    .add("field5", AttributeType.STRING)
    .add("field6", AttributeType.DOUBLE)
    .build()

  def physicalOpId(): PhysicalOpIdentity = {
    counter += 1
    PhysicalOpIdentity(OperatorIdentity("" + counter), "" + counter)
  }

  def mkDataMessage(
      to: ActorVirtualIdentity,
      from: ActorVirtualIdentity,
      seq: Long,
      payload: DataPayload
  ): WorkflowFIFOMessage = {
    WorkflowFIFOMessage(ChannelIdentity(from, to, isControl = false), seq, payload)
  }

  "OutputManager" should "aggregate tuples and output" in {
    val outputManager = wire[OutputManager]

    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    val fakeID = ActorVirtualIdentity("testReceiver")
    inSequence {
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 0, DataFrame(tuples.slice(0, 10)))
      )
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 1, DataFrame(tuples.slice(10, 20)))
      )
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 2, DataFrame(tuples.slice(20, 21)))
      )
      (mockHandler.apply _).expects(mkDataMessage(fakeID, identifier, 3, EndOfUpstream()))
    }
    val fakeLink = PhysicalLink(physicalOpId(), PortIdentity(), physicalOpId(), PortIdentity())
    val fakeReceiver = Array[ActorVirtualIdentity](fakeID)

    outputManager.addPartitionerWithPartitioning(
      fakeLink,
      OneToOnePartitioning(10, fakeReceiver.toSeq)
    )
    tuples.foreach { t =>
      outputManager.passTupleToDownstream(t, fakeLink, schema)
    }
    outputManager.emitEndOfUpstream()
  }

  "OutputManager" should "not output tuples when there is no partitioning" in {
    val outputManager = wire[OutputManager]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    (mockHandler.apply _).expects(*).never()
    val fakeLink = PhysicalLink(physicalOpId(), PortIdentity(), physicalOpId(), PortIdentity())
    assertThrows[Exception] {
      tuples.foreach { t =>
        outputManager.passTupleToDownstream(t, fakeLink, schema)
      }
      outputManager.emitEndOfUpstream()
    }
  }

}
