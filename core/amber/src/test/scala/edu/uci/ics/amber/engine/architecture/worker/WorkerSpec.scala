package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  NetworkInputPort,
  NetworkOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.architecture.worker.workloadmetrics.SelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor, InputExhausted}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkerSpec
    extends TestKit(ActorSystem("WorkerSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with MockFactory {

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Worker" should "process AddDateSendingPolicy message correctly" in {
    val mockHandler =
      mock[(ActorVirtualIdentity, ActorVirtualIdentity, Long, ControlPayload) => Unit]
    val identifier = ActorVirtualIdentity("worker mock")
    val mockControlOutputPort: NetworkOutputPort[ControlPayload] =
      new NetworkOutputPort[ControlPayload](identifier, mockHandler)
    val mockTupleToBatchConverter = mock[TupleToBatchConverter]
    val identifier1 = ActorVirtualIdentity("worker-1")
    val identifier2 = ActorVirtualIdentity("worker-2")
    val mockOpExecutor = new IOperatorExecutor {
      override def open(): Unit = println("opened!")

      override def close(): Unit = println("closed!")

      override def processTuple(
          tuple: Either[ITuple, InputExhausted],
          input: LinkIdentity,
          pauseManager: PauseManager,
          asyncRPCClient: AsyncRPCClient
      ): Iterator[(ITuple, Option[LinkIdentity])] = ???
    }

    val mockTag = LinkIdentity(null, null)

    val mockPolicy = OneToOnePartitioning(10, Array(identifier2))

    val worker = TestActorRef(new WorkflowWorker(identifier1, mockOpExecutor, TestProbe().ref) {
      override lazy val batchProducer: TupleToBatchConverter = mockTupleToBatchConverter
      override lazy val controlOutputPort: NetworkOutputPort[ControlPayload] = mockControlOutputPort
    })
    (mockTupleToBatchConverter.addPartitionerWithPartitioning _).expects(mockTag, mockPolicy).once()
    (mockHandler.apply _).expects(*, *, *, *).once()
    val invocation = ControlInvocation(0, AddPartitioning(mockTag, mockPolicy))
    worker ! NetworkMessage(
      0,
      WorkflowControlMessage(CONTROLLER, 0, invocation)
    )

    //wait test to finish
    Thread.sleep(3000)
  }

  "Worker" should "process monitoring message correctly" in {
    val probe = TestProbe()
    val idMap = mutable.HashMap[ActorVirtualIdentity, ActorRef]()
    val identifier1 = ActorVirtualIdentity("worker-1")
    val mockOpExecutor = new IOperatorExecutor {
      override def open(): Unit = println("opened!")

      override def close(): Unit = println("closed!")

      override def processTuple(
          tuple: Either[ITuple, InputExhausted],
          input: LinkIdentity,
          pauseManager: PauseManager,
          asyncRPCClient: AsyncRPCClient
      ): Iterator[(ITuple, Option[LinkIdentity])] = { return Iterator() }
    }

    val worker = TestActorRef(new WorkflowWorker(identifier1, mockOpExecutor, probe.ref))

    idMap(identifier1) = worker
    idMap(CONTROLLER) = probe.ref

    probe.send(
      worker,
      NetworkMessage(
        0,
        WorkflowControlMessage(
          CONTROLLER,
          0,
          ControlInvocation(0, QuerySelfWorkloadMetrics())
        )
      )
    )

    probe.receiveWhile(1.minutes, 5.seconds) {
      case GetActorRef(id, replyTo) =>
        replyTo.foreach { actor =>
          actor ! RegisterActorRef(id, idMap(id))
        }
      case NetworkMessage(
            msgID,
            WorkflowControlMessage(_, _, ReturnInvocation(id, returnValue))
          ) =>
        probe.sender() ! NetworkAck(msgID, Some(Constants.unprocessedBatchesCreditLimitPerSender))
        returnValue match {
          case e: Throwable => throw e
          case _ =>
            assert(
              returnValue
                .asInstanceOf[Tuple2[
                  SelfWorkloadMetrics,
                  mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
                ]]
                ._1
                .unprocessedDataInputQueueSize == 0
            )
            assert(
              returnValue
                .asInstanceOf[Tuple2[
                  SelfWorkloadMetrics,
                  mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
                ]]
                ._1
                .unprocessedControlInputQueueSize == 0
            )
            assert(
              returnValue
                .asInstanceOf[Tuple2[
                  SelfWorkloadMetrics,
                  mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
                ]]
                ._1
                .stashedDataInputQueueSize == 0
            )
            assert(
              returnValue
                .asInstanceOf[Tuple2[
                  SelfWorkloadMetrics,
                  mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
                ]]
                ._1
                .stashedControlInputQueueSize == 0
            )
        }
      case other =>
      //skip
    }
  }

}
