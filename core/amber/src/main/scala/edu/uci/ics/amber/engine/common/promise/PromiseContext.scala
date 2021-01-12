package edu.uci.ics.amber.engine.common.promise

import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

object PromiseContext {
  def apply(sender: ActorVirtualIdentity, id: Long): RootPromiseContext =
    RootPromiseContext(sender, id)
  def apply(sender: ActorVirtualIdentity, id: Long, root: RootPromiseContext): ChildPromiseContext =
    ChildPromiseContext(sender, id, root)
}

sealed trait PromiseContext {
  def sender: ActorVirtualIdentity
  def id: Long
}

case class RootPromiseContext(sender: ActorVirtualIdentity, id: Long) extends PromiseContext

case class ChildPromiseContext(sender: ActorVirtualIdentity, id: Long, root: RootPromiseContext)
    extends PromiseContext
