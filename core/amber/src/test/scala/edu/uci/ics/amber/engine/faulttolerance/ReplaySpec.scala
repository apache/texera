package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import edu.uci.ics.amber.engine.architecture.logreplay.{
  ProcessingStep,
  ReplayLogManagerImpl,
  ReplayLogRecord,
  ReplayOrderEnforcer
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_START_WORKER
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordReader
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable

class ReplaySpec
    extends TestKit(ActorSystem("ReplaySpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  class IterableReadOnlyLogStore(iter: Iterable[ReplayLogRecord])
      extends SequentialRecordStorage[ReplayLogRecord] {
    override def getWriter(
        fileName: String
    ): SequentialRecordStorage.SequentialRecordWriter[ReplayLogRecord] = ???

    override def getReader(
        fileName: String
    ): SequentialRecordStorage.SequentialRecordReader[ReplayLogRecord] =
      new SequentialRecordReader[ReplayLogRecord](null) {
        override def mkRecordIterator(): Iterator[ReplayLogRecord] = iter.iterator
      }

    override def deleteStorage(): Unit = ???

    override def containsFolder(folderName: String): Boolean = ???
  }

  private val actorId = ActorVirtualIdentity("test")
  private val actorId2 = ActorVirtualIdentity("upstream1")
  private val actorId3 = ActorVirtualIdentity("upstream2")
  private val channelId1 = ChannelIdentity(CONTROLLER, actorId, isControl = true)
  private val channelId2 = ChannelIdentity(actorId2, actorId, isControl = false)
  private val channelId3 = ChannelIdentity(actorId3, actorId, isControl = false)
  private val channelId4 = ChannelIdentity(actorId2, actorId, isControl = true)
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

    def inputMessage(channelId: ChannelIdentity, seq: Long): Unit = {
      inputGateway
        .getChannel(channelId)
        .acceptMessage(
          WorkflowFIFOMessage(
            channelId,
            seq,
            ControlInvocation(
              METHOD_START_WORKER,
              EmptyRequest(),
              AsyncRPCContext(CONTROLLER, actorId),
              0
            )
          )
        )
    }

    val orderEnforcer = new ReplayOrderEnforcer(logManager, logRecords, -1, () => {})
    inputGateway.addEnforcer(orderEnforcer)

    def processMessage(channelId: ChannelIdentity, seq: Long): Unit = {
      val msg = inputGateway.tryPickChannel.get.take
      logManager.withFaultTolerant(msg.channelId, Some(msg)) {
        assert(msg.channelId == channelId && msg.sequenceNumber == seq)
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
