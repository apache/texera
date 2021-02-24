package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.CommandCompleted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

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
    val mockControlOutputPort = mock[ControlOutputPort]
    val mockTupleToBatchConverter = mock[TupleToBatchConverter]
    val identifier1 = WorkerActorVirtualIdentity("worker-1")
    val identifier2 = WorkerActorVirtualIdentity("worker-2")
    val mockOpExecutor = new IOperatorExecutor {
      override def open(): Unit = println("opened!")

      override def close(): Unit = println("closed!")

      override def processTuple(
          tuple: Either[ITuple, InputExhausted],
          input: LinkIdentity
      ): Iterator[ITuple] = ???
    }

    val mockTag = mock[LinkIdentity]

    val mockPolicy = new DataSendingPolicy(mockTag, 10, Array(identifier2)) {
      override def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)] =
        None

      override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = { Array() }

      override def reset(): Unit = {}
    }

    inSequence {
      (mockTupleToBatchConverter.addPolicy _).expects(mockPolicy)
      (mockControlOutputPort.sendTo _)
        .expects(ActorVirtualIdentity.Controller, ReturnPayload(0, CommandCompleted()))
    }

    val worker = TestActorRef(new WorkflowWorker(identifier1, mockOpExecutor, TestProbe().ref) {
      override lazy val batchProducer = mockTupleToBatchConverter
      override lazy val controlOutputPort = mockControlOutputPort
    })
    val invocation = ControlInvocation(0, AddOutputPolicy(mockPolicy))
    worker ! NetworkMessage(
      0,
      WorkflowControlMessage(ActorVirtualIdentity.Controller, 0, invocation)
    )

    //wait test to finish
    Thread.sleep(3000)
  }

}
