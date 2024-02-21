package edu.uci.ics.amber.engine.architecture.control

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  GetActorRef,
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.control.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.control.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.control.utils.ErrorHandler.ErrorCommand
import edu.uci.ics.amber.engine.architecture.control.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.control.utils.NestedHandler.Nested
import edu.uci.ics.amber.engine.architecture.control.utils.PingPongHandler.Ping
import edu.uci.ics.amber.engine.architecture.control.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.architecture.control.utils.TrivialControlTester
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.mutable
import scala.concurrent.duration._

class TrivialControlSpec
    extends TestKit(ActorSystem("TrivialControlSpec"))
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def testControl[T](numActors: Int, eventPairs: (ControlCommand[_], T)*): Unit = {
    val (events, expectedValues) = eventPairs.unzip
    val (probe, idMap) = setUp(numActors, events: _*)
    var flag = 0
    probe.receiveWhile(5.minutes, 10.seconds) {
      case GetActorRef(id, replyTo) =>
        replyTo.foreach { actor =>
          actor ! RegisterActorRef(id, idMap(id))
        }
      case NetworkMessage(
            msgID,
            workflowMsg @ WorkflowFIFOMessage(_, _, ReturnInvocation(id, returnValue))
          ) =>
        probe.sender() ! NetworkAck(
          msgID,
          getInMemSize(workflowMsg),
          0L // no queued credit
        )
        returnValue match {
          case e: Throwable => throw e
          case _            => assert(returnValue.asInstanceOf[T] == expectedValues(id.toInt))
        }
        flag += 1
      case other =>
      //skip
    }
    if (flag != expectedValues.length) {
      throw new AssertionError()
    }
    idMap.foreach { x =>
      x._2 ! PoisonPill
    }
  }

  def setUp(
      numActors: Int,
      cmd: ControlCommand[_]*
  ): (TestProbe, mutable.HashMap[ActorVirtualIdentity, ActorRef]) = {
    val probe = TestProbe()
    val idMap = mutable.HashMap[ActorVirtualIdentity, ActorRef]()
    for (i <- 0 until numActors) {
      val id = ActorVirtualIdentity(s"$i")
      val ref =
        probe.childActorOf(Props(new TrivialControlTester(id)))
      idMap(id) = ref
    }
    idMap(CONTROLLER) = probe.ref
    var seqNum = 0
    cmd.foreach { evt =>
      probe.send(
        idMap(ActorVirtualIdentity("0")),
        NetworkMessage(
          seqNum,
          WorkflowFIFOMessage(
            ChannelIdentity(CONTROLLER, ActorVirtualIdentity("0"), isControl = true),
            seqNum,
            ControlInvocation(seqNum, evt)
          )
        )
      )
      seqNum += 1
    }
    (probe, idMap)
  }

  "testers" should {

    "execute Ping Pong" in {
      testControl(2, (Ping(1, 5, ActorVirtualIdentity("1")), 5))
    }

    "execute Ping Pong 2 times" in {
      testControl(
        2,
        (Ping(1, 4, ActorVirtualIdentity("1")), 4),
        (Ping(10, 13, ActorVirtualIdentity("1")), 13)
      )
    }

    "execute Chain" in {
      testControl(
        10,
        (
          Chain((1 to 9).map(i => ActorVirtualIdentity(i.toString))),
          ActorVirtualIdentity(9.toString)
        )
      )
    }

    "execute Collect" in {
      testControl(
        4,
        (Collect((1 to 3).map(i => ActorVirtualIdentity(i.toString))), "finished")
      )
    }

    "execute RecursiveCall" in {
      testControl(1, (Recursion(0), "0"))
    }

    "execute MultiCall" in {
      testControl(
        10,
        (MultiCall((1 to 9).map(i => ActorVirtualIdentity(i.toString))), "finished")
      )
    }

    "execute NestedCall" in {
      testControl(1, (Nested(5), "Hello World!"))
    }

    "execute ErrorCall" in {
      assertThrows[RuntimeException] {
        testControl(1, (ErrorCommand(), ()))
      }

    }
  }

}
