package edu.uci.ics.amber.engine.architecture.promise

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.Input
import com.twitter.util.{FuturePool, Promise}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkSenderActor.{
  NetworkAck,
  NetworkMessage,
  QueryActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.promise.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.promise.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.promise.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.promise.utils.NestedHandler.Nested
import edu.uci.ics.amber.engine.architecture.promise.utils.PingPongHandler.Ping
import edu.uci.ics.amber.engine.architecture.promise.utils.RPCTester
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}
import edu.uci.ics.amber.engine.common.promise.RPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.mutable
import scala.concurrent.duration._

class RPCSpec
    extends TestKit(ActorSystem("RPCSpec"))
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def setUp(
      numActors: Int,
      event: RPCCommand[_]*
  ): (TestProbe, mutable.HashMap[ActorVirtualIdentity, ActorRef]) = {
    val probe = TestProbe()
    val idMap = mutable.HashMap[ActorVirtualIdentity, ActorRef]()
    for (i <- 0 until numActors) {
      val id = WorkerActorVirtualIdentity(s"$i")
      val ref = probe.childActorOf(Props(new RPCTester(id)))
      idMap(id) = ref
    }
    idMap(VirtualIdentity.Controller) = probe.ref
    var seqNum = 0
    event.foreach { evt =>
      probe.send(
        idMap(WorkerActorVirtualIdentity("0")),
        NetworkMessage(
          seqNum,
          WorkflowControlMessage(
            VirtualIdentity.Controller,
            seqNum,
            ControlInvocation(seqNum, evt)
          )
        )
      )
      seqNum += 1
    }
    (probe, idMap)
  }

  def testPromise[T](numActors: Int, eventPairs: (RPCCommand[_], T)*): Unit = {
    val (events, expectedValues) = eventPairs.unzip
    val (probe, idMap) = setUp(numActors, events: _*)
    var flag = 0
    probe.receiveWhile(5.minutes, 5.seconds) {
      case QueryActorRef(id, replyTo) =>
        replyTo.foreach { actor =>
          actor ! RegisterActorRef(id, idMap(id))
        }
      case NetworkMessage(msgID, WorkflowControlMessage(_, _, ReturnPayload(id, returnValue))) =>
        probe.sender() ! NetworkAck(msgID)
        assert(returnValue.asInstanceOf[T] == expectedValues(id.toInt))
        flag += 1
      case other =>
      //skip
    }
    if (flag != expectedValues.length) {
      throw new AssertionError()
    }
  }

  "testers" should {

    "execute Ping Pong" in {
      testPromise(2, (Ping(1, 5, WorkerActorVirtualIdentity("1")), 5))
    }

    "execute Ping Pong 2 times" in {
      testPromise(
        2,
        (Ping(1, 4, WorkerActorVirtualIdentity("1")), 4),
        (Ping(10, 13, WorkerActorVirtualIdentity("1")), 13)
      )
    }

    "execute Chain" in {
      testPromise(
        10,
        (
          Chain((1 to 9).map(i => WorkerActorVirtualIdentity(i.toString))),
          WorkerActorVirtualIdentity(9.toString)
        )
      )
    }

    "execute Collect" in {
      testPromise(
        4,
        (Collect((1 to 3).map(i => WorkerActorVirtualIdentity(i.toString))), "finished")
      )
    }

    "execute RecursiveCall" in {
      testPromise(1, (Recursion(0), "0"))
    }

    "execute MultiCall" in {
      testPromise(
        10,
        (MultiCall((1 to 9).map(i => WorkerActorVirtualIdentity(i.toString))), "finished")
      )
    }

    "execute NestedCall" in {
      testPromise(1, (Nested(5), "Hello World!"))
    }

  }

}
