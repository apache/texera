package edu.uci.ics.amber.engine.common.virtualidentity

case class WorkflowIdentity(id: String) extends VirtualIdentity {
  override def toString: String = s"Workflow($id)"
}
