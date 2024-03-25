package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{ActorContext, ActorRef, Address, Cancellable, Props}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.FutureBijection._

import scala.concurrent.ExecutionContext

class AkkaActorService(val id: ActorVirtualIdentity, actorContext: ActorContext) {

  implicit def ec: ExecutionContext = actorContext.dispatcher
  implicit val timeout: Timeout = 5.seconds
  implicit val self: ActorRef = actorContext.self

  def parent: ActorRef = actorContext.parent

  private var addresses: Array[Address] = Array()

  def setAvailableNodeAddresses(addresses: Array[Address]): Unit = this.addresses = addresses
  def getClusterNodeAddresses: Array[Address] = {
    addresses
  }

  def actorOf(props: Props): ActorRef = {
    actorContext.actorOf(props)
  }

  def scheduleOnce(delay: FiniteDuration, callable: () => Unit): Cancellable = {
    actorContext.system.scheduler.scheduleOnce(delay) {
      callable()
    }
  }

  def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      callable: () => Unit
  ): Cancellable = {
    actorContext.system.scheduler.scheduleWithFixedDelay(initialDelay, delay)(() => callable())
  }

  def sendToSelfOnce(delay: FiniteDuration, msg: Any): Cancellable = {
    actorContext.system.scheduler.scheduleOnce(delay, actorContext.self, msg)
  }

  def sendToSelfWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      msg: Any
  ): Cancellable = {
    actorContext.system.scheduler.scheduleWithFixedDelay(
      initialDelay,
      delay,
      actorContext.self,
      msg
    )
  }

  def ask(ref: ActorRef, message: Any): com.twitter.util.Future[Any] = {
    akka.pattern.ask(ref, message).asTwitter()
  }

}
