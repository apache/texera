package edu.uci.ics.amber.engine.common.virtualidentity

object ActorVirtualIdentity {
  case class WorkerActorVirtualIdentity(name: String) extends ActorVirtualIdentity
  case class ControllerVirtualIdentity() extends ActorVirtualIdentity
  case class SelfVirtualIdentity() extends ActorVirtualIdentity
  case class ClientVirtualIdentity() extends ActorVirtualIdentity

  lazy val Controller: ControllerVirtualIdentity = ControllerVirtualIdentity()
  lazy val Self: SelfVirtualIdentity = SelfVirtualIdentity()
  lazy val Client: ClientVirtualIdentity = ClientVirtualIdentity()
}

trait ActorVirtualIdentity extends VirtualIdentity
