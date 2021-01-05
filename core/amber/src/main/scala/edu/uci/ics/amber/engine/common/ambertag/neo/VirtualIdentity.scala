package edu.uci.ics.amber.engine.common.ambertag.neo

// The following pattern is a good practice of enum in scala
// We've always used this pattern in the codebase
// https://pedrorijo.com/blog/scala-enums/

trait VirtualIdentity

object VirtualIdentity {

  trait ActorVirtualIdentity extends VirtualIdentity

  case class WorkerActorVirtualIdentity(name: String) extends ActorVirtualIdentity
  case class ControllerVirtualIdentity() extends ActorVirtualIdentity
  case class SelfVirtualIdentity() extends ActorVirtualIdentity

  lazy val Controller: ControllerVirtualIdentity = ControllerVirtualIdentity()
  lazy val Self: SelfVirtualIdentity = SelfVirtualIdentity()
}
