package edu.uci.ics.amber.engine.common.ambertag

case class OperatorIdentifier(workflow: String, operator: String) extends AmberTag {
  override def getGlobalIdentity: String = workflow + "-" + operator

  override def equals(obj: Any): Boolean = {
    obj match {
      case opID: OperatorIdentifier =>
        (opID.workflow == this.workflow && opID.operator == this.operator)
      case _ => false
    }
  }
}

object OperatorIdentifier {
  def apply(workflowTag: WorkflowTag, operator: String): OperatorIdentifier = {
    OperatorIdentifier(workflowTag.workflow, operator)
  }
}
