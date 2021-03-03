package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, WorkflowDataMessage}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class NetworkInputPortSpec extends AnyFlatSpec with MockFactory {

  private val mockHandler = mock[(VirtualIdentity, DataPayload) => Unit]
  private val fakeID = WorkerActorVirtualIdentity("testReceiver")
  private val logger: WorkflowLogger = WorkflowLogger("NetworkInputPortSpec")

  "network input port" should "output payload in FIFO order" in {
    val inputPort = new NetworkInputPort[DataPayload](logger, mockHandler)
    val payloads = (0 until 4).map { i =>
      DataFrame(Array(ITuple(i)))
    }.toArray
    val messages = (0 until 4).map { i =>
      WorkflowDataMessage(fakeID, i, payloads(i))
    }.toArray

    inSequence {
      (0 until 4).foreach(i => {
        (mockHandler.apply _).expects(fakeID, payloads(i))
      })
    }

    List(2, 1, 0, 3).foreach(id => {
      inputPort.handleMessage(
        Option.empty,
        id,
        messages(id).from,
        messages(id).sequenceNumber,
        messages(id).payload
      )
    })
  }

  "network input port" should "de-duplicate payload" in {
    val inputPort = new NetworkInputPort[DataPayload](logger, mockHandler)

    val payload = DataFrame(Array(ITuple(0)))
    val message = WorkflowDataMessage(fakeID, 0, payload)

    inSequence {
      (mockHandler.apply _).expects(fakeID, payload)
      (mockHandler.apply _).expects(*, *).never
    }

    (0 until 10).foreach(_ => {
      inputPort.handleMessage(
        Option.empty,
        0,
        message.from,
        message.sequenceNumber,
        message.payload
      )
    })
  }

  "network input port" should "send ack to the sender actor ref" in {
    val inputPort = new NetworkInputPort[DataPayload](logger, (_, _) => {})

    val a = TestProbe.apply("test")(ActorSystem())

    val payload = DataFrame(Array(ITuple(0)))
    val message = WorkflowDataMessage(fakeID, 0, payload)
    val messageID = 0

    inputPort.handleMessage(
      Option(a.ref),
      messageID,
      message.from,
      message.sequenceNumber,
      message.payload
    )
    a.expectMsg(NetworkAck(0))
  }

}
