package edu.uci.ics.amber.engine.common.ambertag

case class WorkflowTag(workflow: String) extends AmberTag {
  override def getGlobalIdentity: String = workflow
}
