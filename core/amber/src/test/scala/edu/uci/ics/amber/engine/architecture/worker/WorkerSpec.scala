package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
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
//    val mockControlOutputPort = mock[ControlOutputPort]
//    val mockTupleToBatchConverter = mock[TupleToBatchConverter]
//    val identifier1 = WorkerActorVirtualIdentity("worker-1")
//    val identifier2 = WorkerActorVirtualIdentity("worker-2")
//    val mockOpExecutor = mock[IOperatorExecutor]
//    val mockTag = mock[LinkTag]
//
//    val mockPolicy = new DataSendingPolicy(mockTag, 10, Array(identifier2)) {
//      override def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)] =
//        None
//
//      override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = { Array() }
//
//      override def reset(): Unit = {}
//    }
//
//    inAnyOrder {
//      (mockTupleToBatchConverter.addPolicy _).expects(mockPolicy)
//    }
//
//    val worker = TestActorRef(new WorkflowWorker(identifier1, mockOpExecutor, TestProbe().ref) {
//      override lazy val batchProducer = mockTupleToBatchConverter
//      override lazy val controlOutputPort = mockControlOutputPort
//    })
//    worker ! AddOutputPolicy(mockPolicy)
  }

}
