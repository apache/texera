package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.EndOfUpstream
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
    .builder()
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
    val mockPortId = PortIdentity()
    outputManager.addPort(mockPortId, schema)

    val tuples = Array.fill(21)(
      TupleLike(1, 2, 3, 4, "5", 9.8).enforceSchema(schema)
    )
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
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 3, MarkerFrame(EndOfUpstream()))
      )
    }
    val fakeLink = PhysicalLink(physicalOpId(), mockPortId, physicalOpId(), mockPortId)
    val fakeReceiver =
      Array[ChannelIdentity](ChannelIdentity(identifier, fakeID, isControl = false))

    outputManager.addPartitionerWithPartitioning(
      fakeLink,
      OneToOnePartitioning(10, fakeReceiver.toSeq)
    )
    tuples.foreach { t =>
      outputManager.passTupleToDownstream(TupleLike(t.getFields), None)
    }
    outputManager.emitMarker(EndOfUpstream())
  }

}
