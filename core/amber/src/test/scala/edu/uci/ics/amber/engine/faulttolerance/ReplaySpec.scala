package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.ReplayLogReader
import edu.uci.ics.amber.engine.architecture.logreplay.{
  ProcessingStep,
  ReplayLogManagerImpl,
  ReplayLogRecord,
  ReplayOrderEnforcer
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputGateway
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable

class ReplaySpec
    extends TestKit(ActorSystem("ReplaySpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  class IterableReadOnlyLogStore(iter: Iterable[ReplayLogRecord]) extends ReplayLogStorage {
    override def getWriter(logFileName: String): ReplayLogStorage.ReplayLogWriter = ???

    override def getReader(logFileName: String): ReplayLogStorage.ReplayLogReader =
      new ReplayLogReader(null) {
        override def mkLogRecordIterator(): Iterator[ReplayLogRecord] = iter.toIterator
      }

    override def deleteStorage(): Unit = ???
  }

  private val actorId = ActorVirtualIdentity("test")
  private val actorId2 = ActorVirtualIdentity("upstream1")
  private val actorId3 = ActorVirtualIdentity("upstream2")
  private val channelId1 = ChannelID(CONTROLLER, actorId, isControl = true)
  private val channelId2 = ChannelID(actorId2, actorId, isControl = false)
  private val channelId3 = ChannelID(actorId3, actorId, isControl = false)
  private val channelId4 = ChannelID(actorId2, actorId, isControl = true)
  private val logManager = new ReplayLogManagerImpl(x => {})

  "replay input gate" should "replay the message payload in log order" in {
    val logRecords = mutable.Queue[ProcessingStep](
      ProcessingStep(channelId1, -1),
      ProcessingStep(channelId4, 1),
      ProcessingStep(channelId3, 2),
      ProcessingStep(channelId1, 3),
      ProcessingStep(channelId2, 4)
    )
    val inputGateway = new NetworkInputGateway(actorId)
    def inputMessage(channelID: ChannelID, seq: Long): Unit = {
      inputGateway
        .getChannel(channelID)
        .acceptMessage(WorkflowFIFOMessage(channelID, seq, ControlInvocation(0, StartWorker())))
    }
    val orderEnforcer = new ReplayOrderEnforcer(logManager, logRecords, -1, 1000, () => {})
    inputGateway.addEnforcer(orderEnforcer)
    def processMessage(channelID: ChannelID, seq: Long): Unit = {
      val msg = inputGateway.tryPickChannel.get.take
      logManager.withFaultTolerant(msg.channel, Some(msg)) {
        assert(msg.channel == channelID && msg.sequenceNumber == seq)
      }
    }
    assert(inputGateway.tryPickChannel.isEmpty)
    inputMessage(channelId2, 0)
    assert(inputGateway.tryPickChannel.isEmpty)
    inputMessage(channelId4, 0)
    assert(inputGateway.tryPickChannel.isEmpty)
    inputMessage(channelId1, 0)
    inputMessage(channelId1, 1)
    inputMessage(channelId1, 2)
    assert(
      inputGateway.tryPickChannel.nonEmpty && inputGateway.tryPickChannel.get.channelId == channelId1
    )
    processMessage(channelId1, 0)
    assert(inputGateway.tryPickChannel.nonEmpty)
    processMessage(channelId1, 1)
    assert(inputGateway.tryPickChannel.nonEmpty)
    processMessage(channelId4, 0)
    assert(inputGateway.tryPickChannel.isEmpty)
    inputMessage(channelId3, 0)
    processMessage(channelId3, 0)
    assert(inputGateway.tryPickChannel.nonEmpty)
    processMessage(channelId1, 2)
    assert(inputGateway.tryPickChannel.nonEmpty)
    processMessage(channelId2, 0)
  }

}
