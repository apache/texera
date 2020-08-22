package Engine.Common.AmberTag

case class OperatorTag(workflow: String, operator: String) extends AmberTag {
  override def getGlobalIdentity: String = workflow + "-" + operator
}

object OperatorTag {
  def apply(workflowTag: WorkflowTag, operator: String): OperatorTag = {
    OperatorTag(workflowTag.workflow, operator)
  }
}
