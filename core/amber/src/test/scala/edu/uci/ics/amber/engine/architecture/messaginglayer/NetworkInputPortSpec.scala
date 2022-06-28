package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, WorkflowDataMessage}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class NetworkInputPortSpec extends AnyFlatSpec with MockFactory {

  private val mockHandler = mock[(ActorVirtualIdentity, DataPayload) => Unit]
  private val fakeID = ActorVirtualIdentity("testReceiver")

  "network input port" should "output payload in FIFO order" in {
    val testActor = TestProbe.apply("test")(ActorSystem())
    val inputPort = new NetworkInputPort[DataPayload](fakeID, mockHandler)
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
        testActor.ref,
        Constants.unprocessedBatchesCreditLimitPerSender,
        id,
        messages(id).from,
        messages(id).sequenceNumber,
        messages(id).payload
      )
    })
  }

  "network input port" should "de-duplicate payload" in {
    val testActor = TestProbe.apply("test")(ActorSystem())
    val inputPort = new NetworkInputPort[DataPayload](fakeID, mockHandler)

    val payload = DataFrame(Array(ITuple(0)))
    val message = WorkflowDataMessage(fakeID, 0, payload)

    inSequence {
      (mockHandler.apply _).expects(fakeID, payload)
      (mockHandler.apply _).expects(*, *).never
    }

    (0 until 10).foreach(i => {
      inputPort.handleMessage(
        testActor.ref,
        Constants.unprocessedBatchesCreditLimitPerSender,
        i,
        message.from,
        message.sequenceNumber,
        message.payload
      )
    })
  }

  "network input port" should "send ack to the sender actor ref" in {
    val testActor = TestProbe.apply("test")(ActorSystem())
    val inputPort = new NetworkInputPort[DataPayload](fakeID, (_, _) => {})

    val payload = DataFrame(Array(ITuple(0)))
    val message = WorkflowDataMessage(fakeID, 0, payload)
    val messageID = 0

    inputPort.handleMessage(
      testActor.ref,
      Constants.unprocessedBatchesCreditLimitPerSender,
      messageID,
      message.from,
      message.sequenceNumber,
      message.payload
    )
    testActor.expectMsg(
      NetworkAck(messageID, Some(Constants.unprocessedBatchesCreditLimitPerSender))
    )
  }

}
