package Engine.Architecture.SendSemantics.Routees

import Engine.Common.AmberMessage.ControlMessage.{Pause, Resume}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import akka.actor.{Actor, ActorContext, ActorRef, PoisonPill, Props}

import scala.collection.mutable.ArrayBuffer

abstract class ActorRoutee(receiver: ActorRef) extends BaseRoutee(receiver) {

  var senderActor: ActorRef = _
  val stash = new ArrayBuffer[Any]
  var isPaused = false

  override def pause(): Unit = {
    isPaused = true
    senderActor ! Pause
  }

  override def resume()(implicit sender: ActorRef): Unit = {
    senderActor ! Resume
    isPaused = false
    for (i <- stash) {
      i match {
        case d: DataMessage => senderActor ! d
        case e: EndSending  => senderActor ! e
      }
    }
    stash.clear()
  }

  override def schedule(msg: DataMessage)(implicit sender: ActorRef): Unit = {
    if (isPaused) {
      stash.append(msg)
    } else {
      senderActor ! msg
    }
  }

  override def schedule(msg: EndSending)(implicit sender: ActorRef): Unit = {
    if (isPaused) {
      stash.append(msg)
    } else {
      senderActor ! msg
    }
  }

  override def dispose(): Unit = {
    senderActor ! PoisonPill
  }

  override def reset(): Unit = {
    senderActor ! PoisonPill
    stash.clear()
    isPaused = false
  }
}
