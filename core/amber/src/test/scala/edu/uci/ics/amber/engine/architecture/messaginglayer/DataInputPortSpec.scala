package edu.uci.ics.amber.engine.architecture.messaginglayer

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.common.ambermessage.DataFrame
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.recovery.DataLogManager.DataLogElement
import edu.uci.ics.amber.engine.recovery.{ControlLogManager, DataLogManager, EmptyLogStorage}

class DataInputPortSpec extends AnyFlatSpec with MockFactory {

  private val mockBatchToTupleConverter = mock[BatchToTupleConverter]
  private val fakeID = WorkerActorVirtualIdentity("testReceiver")
  private val controlInputPort = mock[ControlInputPort]
  private val storage = new EmptyLogStorage[DataLogElement]
  private val logManager = wire[DataLogManager]

  "data input port" should "output data in FIFO order" in {
    val inputPort = wire[DataInputPort]
    val payloads = (0 until 4).map { i =>
      DataFrame(Array(ITuple(i)))
    }.toArray
    val messages = (0 until 4).map { i =>
      WorkflowDataMessage(fakeID, i, payloads(i))
    }.toArray
    inSequence {
      payloads.map { x =>
        (mockBatchToTupleConverter.processDataPayload _).expects(fakeID, x)
      }
    }

    inputPort.handleDataMessage(messages(2))
    inputPort.handleDataMessage(messages(1))
    inputPort.handleDataMessage(messages(0))
    inputPort.handleDataMessage(messages(3))
  }

  "data input port" should "de-duplicate data " in {
    val inputPort = wire[DataInputPort]
    val payload = DataFrame(Array(ITuple(0)))
    val message = WorkflowDataMessage(fakeID, 0, payload)
    inSequence {
      (mockBatchToTupleConverter.processDataPayload _).expects(fakeID, payload)
      (mockBatchToTupleConverter.processDataPayload _).expects(*, *).never
    }
    inputPort.handleDataMessage(message)
    inputPort.handleDataMessage(message)
    inputPort.handleDataMessage(message)
    inputPort.handleDataMessage(message)
    inputPort.handleDataMessage(message)
  }

}
